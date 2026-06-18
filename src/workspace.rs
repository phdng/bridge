use std::{
    env,
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tokio::fs;

#[derive(Clone)]
pub struct Workspace {
    root: PathBuf,
}

#[derive(Deserialize)]
pub struct FileQuery {
    pub root: String,
    #[serde(default)]
    pub path: String,
}

#[derive(Deserialize)]
pub struct WriteFileRequest {
    pub root: String,
    pub path: String,
    pub content: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub name: String,
    pub path: String,
    pub kind: String,
    pub size: u64,
    pub modified_ms: u128,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileListResponse {
    pub root: String,
    pub path: String,
    pub entries: Vec<FileEntry>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileReadResponse {
    pub root: String,
    pub path: String,
    pub content: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileWriteResponse {
    pub ok: bool,
    pub backup_path: Option<String>,
}

impl Workspace {
    pub fn new() -> std::io::Result<Self> {
        let root = match env::var("TANGO_WORKSPACE") {
            Ok(path) if !path.trim().is_empty() => PathBuf::from(path),
            _ => env::current_dir()?.join("workspace"),
        };

        for name in ["scripts", "templates", "logs"] {
            std::fs::create_dir_all(root.join(name))?;
        }

        Ok(Self { root })
    }

    pub async fn list(&self, query: FileQuery) -> Result<FileListResponse, WorkspaceError> {
        let root_name = normalize_root(&query.root)?;
        let relative = normalize_relative_path(&query.path)?;
        let dir = self.resolve(root_name, &relative)?;
        let mut reader = fs::read_dir(&dir).await.map_err(WorkspaceError::io)?;
        let mut entries = Vec::new();

        while let Some(entry) = reader.next_entry().await.map_err(WorkspaceError::io)? {
            let metadata = entry.metadata().await.map_err(WorkspaceError::io)?;
            let name = entry.file_name().to_string_lossy().to_string();
            let child_relative = join_web_path(&relative, &name);
            entries.push(FileEntry {
                name,
                path: child_relative,
                kind: if metadata.is_dir() {
                    "directory"
                } else {
                    "file"
                }
                .to_string(),
                size: metadata.len(),
                modified_ms: metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .map(|duration| duration.as_millis())
                    .unwrap_or_default(),
            });
        }

        entries.sort_by(|a, b| a.kind.cmp(&b.kind).then_with(|| a.name.cmp(&b.name)));

        Ok(FileListResponse {
            root: root_name.to_string(),
            path: relative_to_web_path(&relative),
            entries,
        })
    }

    pub async fn read_file(&self, query: FileQuery) -> Result<FileReadResponse, WorkspaceError> {
        let root_name = normalize_root(&query.root)?;
        let relative = normalize_relative_path(&query.path)?;
        let path = self.resolve(root_name, &relative)?;
        let content = fs::read_to_string(&path).await.map_err(WorkspaceError::io)?;
        Ok(FileReadResponse {
            root: root_name.to_string(),
            path: relative_to_web_path(&relative),
            content,
        })
    }

    pub async fn write_file(
        &self,
        request: WriteFileRequest,
    ) -> Result<FileWriteResponse, WorkspaceError> {
        let root_name = normalize_root(&request.root)?;
        let relative = normalize_relative_path(&request.path)?;
        if relative.as_os_str().is_empty() {
            return Err(WorkspaceError::bad_request("file path is required"));
        }

        let path = self.resolve(root_name, &relative)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(WorkspaceError::io)?;
        }

        let backup_path = if fs::metadata(&path).await.is_ok() {
            let backup = path.with_extension(match path.extension().and_then(|s| s.to_str()) {
                Some(ext) if !ext.is_empty() => format!("{ext}.bak"),
                _ => "bak".to_string(),
            });
            fs::copy(&path, &backup).await.map_err(WorkspaceError::io)?;
            self.write_history_copy(root_name, &relative, &path).await?;
            Some(path_to_web_path(root_name, &backup, &self.root)?)
        } else {
            None
        };

        fs::write(&path, request.content).await.map_err(WorkspaceError::io)?;
        Ok(FileWriteResponse {
            ok: true,
            backup_path,
        })
    }

    pub async fn delete_path(&self, query: FileQuery) -> Result<FileWriteResponse, WorkspaceError> {
        let root_name = normalize_root(&query.root)?;
        let relative = normalize_relative_path(&query.path)?;
        if relative.as_os_str().is_empty() {
            return Err(WorkspaceError::bad_request("path is required"));
        }

        let path = self.resolve(root_name, &relative)?;
        let metadata = fs::metadata(&path).await.map_err(WorkspaceError::io)?;
        if metadata.is_dir() {
            fs::remove_dir_all(&path).await.map_err(WorkspaceError::io)?;
        } else {
            fs::remove_file(&path).await.map_err(WorkspaceError::io)?;
        }

        Ok(FileWriteResponse {
            ok: true,
            backup_path: None,
        })
    }

    fn resolve(&self, root_name: &str, relative: &Path) -> Result<PathBuf, WorkspaceError> {
        let base = self.root.join(root_name);
        let full = base.join(relative);
        if !full.starts_with(&base) {
            return Err(WorkspaceError::forbidden("path escapes workspace"));
        }
        Ok(full)
    }

    async fn write_history_copy(
        &self,
        root_name: &str,
        relative: &Path,
        source: &Path,
    ) -> Result<(), WorkspaceError> {
        let history_dir = self.root.join(root_name).join("history");
        fs::create_dir_all(&history_dir).await.map_err(WorkspaceError::io)?;
        let file_name = relative
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| WorkspaceError::bad_request("invalid file name"))?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default();
        let history_name = format!("{timestamp}.{file_name}");
        fs::copy(source, history_dir.join(history_name))
            .await
            .map_err(WorkspaceError::io)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct WorkspaceError {
    status: StatusCode,
    message: String,
}

impl WorkspaceError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn io(error: std::io::Error) -> Self {
        let status = match error.kind() {
            std::io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
            std::io::ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for WorkspaceError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

fn normalize_root(root: &str) -> Result<&'static str, WorkspaceError> {
    match root {
        "scripts" => Ok("scripts"),
        "templates" => Ok("templates"),
        "logs" => Ok("logs"),
        _ => Err(WorkspaceError::bad_request(
            "root must be scripts, templates, or logs",
        )),
    }
}

fn normalize_relative_path(path: &str) -> Result<PathBuf, WorkspaceError> {
    let trimmed = path.trim().trim_start_matches('/');
    let mut out = PathBuf::new();
    if trimmed.is_empty() {
        return Ok(out);
    }

    for component in Path::new(trimmed).components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            _ => return Err(WorkspaceError::forbidden("path traversal is not allowed")),
        }
    }
    Ok(out)
}

fn relative_to_web_path(path: &Path) -> String {
    if path.as_os_str().is_empty() {
        return "/".to_string();
    }
    format!("/{}", path.to_string_lossy().replace('\\', "/"))
}

fn join_web_path(parent: &Path, name: &str) -> String {
    let mut path = parent.to_path_buf();
    path.push(name);
    relative_to_web_path(&path)
}

fn path_to_web_path(root_name: &str, path: &Path, workspace_root: &Path) -> Result<String, WorkspaceError> {
    let root = workspace_root.join(root_name);
    let relative = path.strip_prefix(root).map_err(|_| WorkspaceError::forbidden("path escapes workspace"))?;
    Ok(relative_to_web_path(relative))
}
