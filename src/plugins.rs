use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use crate::error::{HarliteError, Result};
use crate::har::{Entry, Har};

pub const PLUGIN_API_VERSION: &str = "v1";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PluginKind {
    Filter,
    Transform,
    Exporter,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PluginPhase {
    Import,
    Export,
    Both,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PluginConfig {
    pub name: String,
    pub kind: PluginKind,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub enabled: Option<bool>,
    pub phase: Option<PluginPhase>,
    pub timeout_secs: Option<u64>,
    pub max_output_bytes: Option<usize>,
}

impl PluginConfig {
    fn effective_phase(&self) -> PluginPhase {
        self.phase.unwrap_or(match self.kind {
            PluginKind::Exporter => PluginPhase::Export,
            _ => PluginPhase::Import,
        })
    }

    fn matches_phase(&self, phase: PluginPhase) -> bool {
        match self.effective_phase() {
            PluginPhase::Both => true,
            other => other == phase,
        }
    }
}

#[derive(Clone, Default)]
pub struct PluginSet {
    plugins: Arc<Vec<PluginConfig>>,
}

impl PluginSet {
    pub fn new(plugins: Vec<PluginConfig>) -> Self {
        Self {
            plugins: Arc::new(plugins),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.plugins.is_empty()
    }

    fn iter_kind_phase(
        &self,
        kind: PluginKind,
        phase: PluginPhase,
    ) -> impl Iterator<Item = &PluginConfig> {
        self.plugins
            .iter()
            .filter(move |plugin| plugin.kind == kind && plugin.matches_phase(phase))
    }

    pub fn apply_import_entry(
        &self,
        entry: &Entry,
        context: &PluginContext<'_>,
    ) -> Result<Option<Entry>> {
        if self.is_empty() {
            return Ok(Some(entry.clone()));
        }

        let mut current = entry.clone();
        for plugin in self.iter_kind_phase(PluginKind::Filter, PluginPhase::Import) {
            if !run_filter_plugin(plugin, &current, context, PluginPhase::Import)? {
                return Ok(None);
            }
        }
        for plugin in self.iter_kind_phase(PluginKind::Transform, PluginPhase::Import) {
            if let Some(next) =
                run_transform_plugin(plugin, &current, context, PluginPhase::Import)?
            {
                current = next;
            }
        }
        Ok(Some(current))
    }

    pub fn apply_export_entry(
        &self,
        mut entry: Entry,
        context: &PluginContext<'_>,
    ) -> Result<Option<Entry>> {
        if self.is_empty() {
            return Ok(Some(entry));
        }

        for plugin in self.iter_kind_phase(PluginKind::Filter, PluginPhase::Export) {
            if !run_filter_plugin(plugin, &entry, context, PluginPhase::Export)? {
                return Ok(None);
            }
        }
        for plugin in self.iter_kind_phase(PluginKind::Transform, PluginPhase::Export) {
            if let Some(next) = run_transform_plugin(plugin, &entry, context, PluginPhase::Export)?
            {
                entry = next;
            }
        }
        Ok(Some(entry))
    }

    pub fn run_exporters(&self, har: &Har, context: &PluginContext<'_>) -> Result<ExporterOutcome> {
        let mut ran = false;
        let mut skip_default = false;
        for plugin in self.iter_kind_phase(PluginKind::Exporter, PluginPhase::Export) {
            ran = true;
            let result = run_exporter_plugin(plugin, har, context)?;
            if result.skip_default.unwrap_or(false) {
                skip_default = true;
            }
        }
        Ok(ExporterOutcome { ran, skip_default })
    }
}

pub struct ExporterOutcome {
    pub ran: bool,
    pub skip_default: bool,
}

#[derive(Serialize)]
pub struct PluginContext<'a> {
    pub command: &'static str,
    pub source: Option<&'a str>,
    pub database: Option<&'a str>,
    pub output: Option<&'a str>,
}

#[derive(Serialize)]
struct PluginRequest<'a> {
    api_version: &'static str,
    event: &'static str,
    phase: PluginPhase,
    context: &'a PluginContext<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    entry: Option<&'a Entry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    har: Option<&'a Har>,
}

#[derive(Deserialize)]
struct FilterResponse {
    allow: Option<bool>,
}

#[derive(Deserialize)]
struct TransformResponse {
    entry: Option<Entry>,
}

#[derive(Deserialize)]
struct ExporterResponse {
    skip_default: Option<bool>,
}

pub fn resolve_plugins(
    configs: &[PluginConfig],
    enabled: &[String],
    disabled: &[String],
) -> Result<PluginSet> {
    let known: HashSet<&str> = configs.iter().map(|c| c.name.as_str()).collect();
    for name in enabled {
        if !known.contains(name.as_str()) {
            return Err(HarliteError::InvalidArgs(format!(
                "Unknown plugin '{}'",
                name
            )));
        }
    }
    for name in disabled {
        if !known.contains(name.as_str()) {
            return Err(HarliteError::InvalidArgs(format!(
                "Unknown plugin '{}'",
                name
            )));
        }
    }

    let enabled_set: HashSet<&str> = enabled.iter().map(|s| s.as_str()).collect();
    let disabled_set: HashSet<&str> = disabled.iter().map(|s| s.as_str()).collect();
    let mut resolved = Vec::new();
    for plugin in configs {
        // Plugin commands may come from project-local configuration. Requiring
        // an explicit CLI enable prevents merely entering a directory from
        // authorizing arbitrary command execution.
        let is_enabled = enabled_set.contains(plugin.name.as_str())
            && plugin.enabled != Some(false)
            && !disabled_set.contains(plugin.name.as_str());
        if is_enabled {
            resolved.push(plugin.clone());
        }
    }

    Ok(PluginSet::new(resolved))
}

fn run_filter_plugin(
    plugin: &PluginConfig,
    entry: &Entry,
    context: &PluginContext<'_>,
    phase: PluginPhase,
) -> Result<bool> {
    let request = PluginRequest {
        api_version: PLUGIN_API_VERSION,
        event: "filter_entry",
        phase,
        context,
        entry: Some(entry),
        har: None,
    };
    let response: FilterResponse = run_plugin(plugin, &request)?;
    response.allow.ok_or_else(|| {
        HarliteError::InvalidArgs(format!(
            "Plugin '{}' did not return an 'allow' field",
            plugin.name
        ))
    })
}

fn run_transform_plugin(
    plugin: &PluginConfig,
    entry: &Entry,
    context: &PluginContext<'_>,
    phase: PluginPhase,
) -> Result<Option<Entry>> {
    let request = PluginRequest {
        api_version: PLUGIN_API_VERSION,
        event: "transform_entry",
        phase,
        context,
        entry: Some(entry),
        har: None,
    };
    let response: TransformResponse = run_plugin(plugin, &request)?;
    Ok(response.entry)
}

fn run_exporter_plugin(
    plugin: &PluginConfig,
    har: &Har,
    context: &PluginContext<'_>,
) -> Result<ExporterResponse> {
    let request = PluginRequest {
        api_version: PLUGIN_API_VERSION,
        event: "export",
        phase: PluginPhase::Export,
        context,
        entry: None,
        har: Some(har),
    };
    run_plugin(plugin, &request)
}

enum PluginIo {
    Input(std::io::Result<()>),
    Stdout(std::io::Result<Vec<u8>>),
    Stderr(std::io::Result<Vec<u8>>),
}

fn read_plugin_output(reader: impl Read, limit: usize) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(std::io::Error::other(format!(
            "output exceeded {limit} bytes"
        )));
    }
    Ok(bytes)
}

struct PluginProcess {
    child: Child,
    complete: bool,
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        if !self.complete {
            #[cfg(unix)]
            // SAFETY: the child starts in a new process group whose ID is its
            // PID. Kill that group so descendants cannot retain the pipe ends.
            unsafe {
                libc::kill(-(self.child.id() as i32), libc::SIGKILL);
            }
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn run_plugin<T: Serialize, R: for<'de> Deserialize<'de>>(
    plugin: &PluginConfig,
    request: &T,
) -> Result<R> {
    let timeout = plugin.timeout_secs.unwrap_or(30);
    let output_limit = plugin.max_output_bytes.unwrap_or(8 * 1024 * 1024);
    if !(1..=86400).contains(&timeout) || !(1..=512 * 1024 * 1024).contains(&output_limit) {
        return Err(HarliteError::InvalidArgs(format!(
            "Plugin '{}' requires timeout_secs in 1..=86400 and max_output_bytes in 1..=536870912",
            plugin.name
        )));
    }
    let mut payload = serde_json::to_vec(request)?;
    payload.push(b'\n');
    let mut cmd = Command::new(&plugin.command);
    cmd.args(&plugin.args);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            HarliteError::InvalidArgs(format!("Failed to spawn plugin '{}': {err}", plugin.name))
        })?;
    let mut process = PluginProcess {
        child,
        complete: false,
    };
    let mut stdin = process
        .child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("Missing plugin stdin"))?;
    let stdout = process
        .child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("Missing plugin stdout"))?;
    let stderr = process
        .child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("Missing plugin stderr"))?;
    let (sender, receiver) = mpsc::channel();
    let input_sender = sender.clone();
    std::thread::spawn(move || {
        let result = stdin.write_all(&payload);
        drop(stdin);
        let _ = input_sender.send(PluginIo::Input(result));
    });
    let output_sender = sender.clone();
    std::thread::spawn(move || {
        let _ = output_sender.send(PluginIo::Stdout(read_plugin_output(stdout, output_limit)));
    });
    std::thread::spawn(move || {
        let _ = sender.send(PluginIo::Stderr(read_plugin_output(stderr, output_limit)));
    });
    let deadline = Instant::now() + Duration::from_secs(timeout);
    let (mut written, mut stdout, mut stderr, mut status) = (false, None, None, None);
    while !written || stdout.is_none() || stderr.is_none() || status.is_none() {
        if Instant::now() >= deadline {
            return Err(HarliteError::InvalidArgs(format!(
                "Plugin '{}' timed out after {timeout} seconds",
                plugin.name
            )));
        }
        if status.is_none() {
            status = process.child.try_wait()?;
        }
        let io_error = |stream, error| {
            HarliteError::InvalidArgs(format!("Plugin '{}' {stream}: {error}", plugin.name))
        };
        match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(PluginIo::Input(result)) => {
                result.map_err(|e| io_error("stdin", e))?;
                written = true;
            }
            Ok(PluginIo::Stdout(result)) => {
                stdout = Some(result.map_err(|e| io_error("stdout", e))?)
            }
            Ok(PluginIo::Stderr(result)) => {
                stderr = Some(result.map_err(|e| io_error("stderr", e))?)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => (),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                std::thread::sleep(Duration::from_millis(10))
            }
        }
    }
    process.complete = true;
    let output = std::process::Output {
        status: status.ok_or_else(|| std::io::Error::other("Missing plugin exit status"))?,
        stdout: stdout.unwrap_or_default(),
        stderr: stderr.unwrap_or_default(),
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let detail = if stderr.is_empty() {
            format!("Plugin '{}' exited with {}", plugin.name, output.status)
        } else {
            format!(
                "Plugin '{}' failed ({}): {}",
                plugin.name, output.status, stderr
            )
        };
        return Err(HarliteError::InvalidArgs(detail));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Err(HarliteError::InvalidArgs(format!(
            "Plugin '{}' returned empty output",
            plugin.name
        )));
    }
    serde_json::from_str(trimmed).map_err(|err| {
        HarliteError::InvalidArgs(format!(
            "Plugin '{}' returned invalid JSON: {}",
            plugin.name, err
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{resolve_plugins, PluginConfig, PluginKind};

    fn config(enabled: Option<bool>) -> PluginConfig {
        PluginConfig {
            name: "test".to_string(),
            kind: PluginKind::Filter,
            command: "ignored".to_string(),
            args: Vec::new(),
            enabled,
            phase: None,
            timeout_secs: None,
            max_output_bytes: None,
        }
    }

    #[cfg(unix)]
    fn shell_plugin(script: &str) -> PluginConfig {
        let mut plugin = config(None);
        plugin.command = "sh".into();
        plugin.args = vec!["-c".into(), script.into()];
        plugin.timeout_secs = Some(1);
        plugin.max_output_bytes = Some(512 * 1024);
        plugin
    }

    #[cfg(unix)]
    #[test]
    fn large_plugin_stdin_and_stderr_do_not_deadlock() {
        let plugin =
            shell_plugin("head -c 262144 /dev/zero >&2; cat >/dev/null; printf '{\"allow\":true}'");
        let response: serde_json::Value = super::run_plugin(&plugin, &"x".repeat(262144)).unwrap();
        assert_eq!(response["allow"], true);
    }

    #[cfg(unix)]
    #[test]
    fn plugin_timeout_also_closes_descendant_pipes() {
        let plugin = shell_plugin("cat >/dev/null; sleep 20 & exit 0");
        let started = std::time::Instant::now();
        let result = super::run_plugin::<_, serde_json::Value>(&plugin, &"input");
        let error = result.unwrap_err().to_string();
        assert!(error.contains("timed out"), "{error}");
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
    }

    #[cfg(unix)]
    #[test]
    fn oversized_plugin_output_is_rejected() {
        let mut plugin = shell_plugin("head -c 262144 /dev/zero; cat >/dev/null");
        plugin.max_output_bytes = Some(1024);
        let error = super::run_plugin::<_, serde_json::Value>(&plugin, &"input").unwrap_err();
        assert!(error.to_string().contains("output exceeded 1024 bytes"));
    }

    #[test]
    fn config_never_enables_command_execution_implicitly() {
        assert!(resolve_plugins(&[config(None)], &[], &[])
            .unwrap()
            .is_empty());
        assert!(resolve_plugins(&[config(Some(true))], &[], &[])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn explicit_cli_enable_is_required_and_config_can_disable() {
        assert!(
            !resolve_plugins(&[config(None)], &["test".to_string()], &[])
                .unwrap()
                .is_empty()
        );
        assert!(
            resolve_plugins(&[config(Some(false))], &["test".to_string()], &[])
                .unwrap()
                .is_empty()
        );
    }
}
