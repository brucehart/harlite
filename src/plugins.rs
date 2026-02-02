use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;

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
}

impl PluginConfig {
    fn effective_phase(&self) -> PluginPhase {
        self.phase.unwrap_or_else(|| match self.kind {
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

    fn iter_kind_phase<'a>(
        &'a self,
        kind: PluginKind,
        phase: PluginPhase,
    ) -> impl Iterator<Item = &'a PluginConfig> {
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
            if let Some(next) =
                run_transform_plugin(plugin, &entry, context, PluginPhase::Export)?
            {
                entry = next;
            }
        }
        Ok(Some(entry))
    }

    pub fn run_exporters(
        &self,
        har: &Har,
        context: &PluginContext<'_>,
    ) -> Result<ExporterOutcome> {
        let mut ran = false;
        let mut skip_default = false;
        for plugin in self.iter_kind_phase(PluginKind::Exporter, PluginPhase::Export) {
            ran = true;
            let result = run_exporter_plugin(plugin, har, context)?;
            if result.skip_default.unwrap_or(false) {
                skip_default = true;
            }
        }
        Ok(ExporterOutcome {
            ran,
            skip_default,
        })
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
        let mut is_enabled = plugin.enabled.unwrap_or(true);
        if disabled_set.contains(plugin.name.as_str()) {
            is_enabled = false;
        }
        if enabled_set.contains(plugin.name.as_str()) {
            is_enabled = true;
        }
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

fn run_plugin<T: Serialize, R: for<'de> Deserialize<'de>>(
    plugin: &PluginConfig,
    request: &T,
) -> Result<R> {
    let payload = serde_json::to_string(request)?;

    let mut cmd = Command::new(&plugin.command);
    if !plugin.args.is_empty() {
        cmd.args(&plugin.args);
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            HarliteError::InvalidArgs(format!(
                "Failed to spawn plugin '{}': {}",
                plugin.name, err
            ))
        })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(payload.as_bytes())?;
        stdin.write_all(b"\n")?;
    }

    let output = child.wait_with_output()?;
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
