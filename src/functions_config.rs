//! Local-only Firebase project and Functions configuration discovery.
//!
//! This module never invokes Firebase/GCP CLIs, metadata servers, secret managers,
//! or other network services. Callers pass command-line overrides and a snapshot of
//! their environment so precedence is deterministic and straightforward to test.

use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
};

type ConfigError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug, Default)]
pub(crate) struct ConfigOverrides {
    pub project: Option<String>,
    pub host: Option<String>,
    pub functions_port: Option<u16>,
    pub pubsub_port: Option<u16>,
    pub functions_source: Option<PathBuf>,
    pub functions_codebase: Option<String>,
    pub functions_runtime: Option<String>,
    pub runtime_config: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FunctionCodebase {
    pub source: PathBuf,
    pub codebase: String,
    /// Firebase runtime spelling (`nodejs18`, `nodejs20`, or `nodejs22`).
    pub runtime: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EmulatorAddresses {
    pub functions: SocketAddr,
    pub firestore: SocketAddr,
    pub auth: SocketAddr,
    pub storage: SocketAddr,
    pub database: SocketAddr,
    pub pubsub: SocketAddr,
}

#[derive(Clone, Debug)]
pub(crate) struct FunctionsConfig {
    pub project_id: String,
    pub codebases: Vec<FunctionCodebase>,
    pub addresses: EmulatorAddresses,
    pub firebase_config: Value,
    pub runtime_config: Value,
    /// Variables from local dotenv files, with matching caller environment values
    /// overlaid. Arbitrary host variables are deliberately not copied.
    pub local_environment: BTreeMap<String, String>,
}

impl FunctionsConfig {
    pub(crate) fn child_environment(&self) -> BTreeMap<String, String> {
        let mut result = self.local_environment.clone();
        result.insert("GCLOUD_PROJECT".into(), self.project_id.clone());
        result.insert("GOOGLE_CLOUD_PROJECT".into(), self.project_id.clone());
        result.insert("FIREBASE_CONFIG".into(), self.firebase_config.to_string());
        result.insert(
            "CLOUD_RUNTIME_CONFIG".into(),
            self.runtime_config.to_string(),
        );
        result.insert(
            "FIRESTORE_EMULATOR_HOST".into(),
            self.addresses.firestore.to_string(),
        );
        result.insert(
            "FIREBASE_AUTH_EMULATOR_HOST".into(),
            self.addresses.auth.to_string(),
        );
        result.insert(
            "FIREBASE_STORAGE_EMULATOR_HOST".into(),
            self.addresses.storage.to_string(),
        );
        result.insert(
            "FIREBASE_DATABASE_EMULATOR_HOST".into(),
            self.addresses.database.to_string(),
        );
        result.insert(
            "PUBSUB_EMULATOR_HOST".into(),
            self.addresses.pubsub.to_string(),
        );
        result.insert("FUNCTIONS_EMULATOR".into(), "true".into());
        result.insert("NODE_ENV".into(), "development".into());
        result
    }
}

pub(crate) fn load(
    root: &Path,
    cli: &ConfigOverrides,
    environment: &BTreeMap<String, String>,
) -> Result<FunctionsConfig, ConfigError> {
    let root = root.canonicalize()?;
    let firebase = read_optional_json(&root.join("firebase.json"))?.unwrap_or_else(|| json!({}));
    let firebaserc = read_optional_json(&root.join(".firebaserc"))?.unwrap_or_else(|| json!({}));

    let project_candidate = cli
        .project
        .clone()
        .or_else(|| environment.get("FIREBASE_EMU_PROJECT").cloned())
        .or_else(|| environment.get("GCLOUD_PROJECT").cloned())
        .or_else(|| environment.get("GOOGLE_CLOUD_PROJECT").cloned())
        .or_else(|| {
            firebaserc
                .pointer("/projects/default")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "demo-rust-emu".into());
    let project_id = firebaserc
        .get("projects")
        .and_then(Value::as_object)
        .and_then(|projects| projects.get(&project_candidate))
        .and_then(Value::as_str)
        .unwrap_or(&project_candidate)
        .to_owned();
    validate_demo_project(&project_id)?;

    let explicit_host = cli
        .host
        .clone()
        .or_else(|| environment.get("FIREBASE_EMU_HOST").cloned());
    let host = if let Some(host) = explicit_host.as_deref() {
        parse_loopback_host(host, false)?
    } else if let Some(host) = firebase
        .pointer("/emulators/functions/host")
        .and_then(Value::as_str)
    {
        parse_loopback_host(host, true)?
    } else {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    };

    let environment_functions_port = env_port(environment, "FIREBASE_FUNCTIONS_EMU_PORT")?;
    let functions_port = cli
        .functions_port
        .or(environment_functions_port)
        .or(json_port(&firebase, "functions")?)
        .unwrap_or(5001);
    if functions_port == 0 {
        return Err("Functions emulator port must be between 1 and 65535".into());
    }
    let addresses = EmulatorAddresses {
        functions: SocketAddr::new(host, functions_port),
        firestore: SocketAddr::new(
            host,
            selected_port(
                environment,
                &firebase,
                "FIRESTORE_EMU_PORT",
                "firestore",
                8080,
            )?,
        ),
        auth: SocketAddr::new(
            host,
            selected_port(
                environment,
                &firebase,
                "FIREBASE_AUTH_EMU_PORT",
                "auth",
                9099,
            )?,
        ),
        storage: SocketAddr::new(
            host,
            selected_port(
                environment,
                &firebase,
                "FIREBASE_STORAGE_EMU_PORT",
                "storage",
                9199,
            )?,
        ),
        database: SocketAddr::new(
            host,
            selected_port(
                environment,
                &firebase,
                "FIREBASE_DATABASE_EMU_PORT",
                "database",
                9000,
            )?,
        ),
        pubsub: SocketAddr::new(
            if explicit_host.is_some() {
                host
            } else if let Some(pubsub_host) = firebase
                .pointer("/emulators/pubsub/host")
                .and_then(Value::as_str)
            {
                parse_loopback_host(pubsub_host, true)?
            } else {
                host
            },
            cli.pubsub_port.unwrap_or(selected_port(
                environment,
                &firebase,
                "PUBSUB_EMULATOR_PORT",
                "pubsub",
                8085,
            )?),
        ),
    };

    let source_override = cli.functions_source.clone().or_else(|| {
        environment
            .get("FIREBASE_FUNCTIONS_SOURCE")
            .map(PathBuf::from)
    });
    let runtime_override = cli
        .functions_runtime
        .clone()
        .or_else(|| environment.get("FIREBASE_FUNCTIONS_RUNTIME").cloned());
    let codebase_override = cli
        .functions_codebase
        .clone()
        .or_else(|| environment.get("FIREBASE_FUNCTIONS_CODEBASE").cloned());
    let entries = if let Some(source) = source_override {
        vec![json!({"source": source, "codebase": codebase_override, "runtime": runtime_override})]
    } else {
        match firebase.get("functions") {
            Some(Value::Array(items)) => items.clone(),
            Some(Value::Object(_)) => vec![firebase["functions"].clone()],
            Some(_) => return Err("firebase.json `functions` must be an object or array".into()),
            None => vec![json!({"source":"functions"})],
        }
    };
    if entries.is_empty() {
        return Err("firebase.json `functions` array must not be empty".into());
    }
    let mut seen = BTreeSet::new();
    let mut codebases = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let object = entry
            .as_object()
            .ok_or("each Functions configuration must be an object")?;
        let configured_source = object
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or("functions");
        let source = confined_source(&root, Path::new(configured_source))?;
        let codebase = codebase_override
            .clone()
            .or_else(|| {
                object
                    .get("codebase")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| {
                if entries.len() == 1 {
                    "default".into()
                } else {
                    format!("codebase-{index}")
                }
            });
        validate_codebase(&codebase)?;
        if !seen.insert(codebase.clone()) {
            return Err(format!("duplicate Functions codebase `{codebase}`").into());
        }
        let runtime_value = runtime_override
            .as_deref()
            .or_else(|| object.get("runtime").and_then(Value::as_str));
        let runtime = resolve_runtime(runtime_value, &source)?;
        codebases.push(FunctionCodebase {
            source,
            codebase,
            runtime,
        });
    }

    let runtime_config = if let Some(value) = &cli.runtime_config {
        Value::Object(validate_object(value, "command-line runtime config")?.clone())
    } else if let Some(raw) = environment.get("FIREBASE_EMU_RUNTIME_CONFIG") {
        let value: Value = serde_json::from_str(raw)
            .map_err(|error| format!("malformed FIREBASE_EMU_RUNTIME_CONFIG: {error}"))?;
        Value::Object(validate_object(&value, "FIREBASE_EMU_RUNTIME_CONFIG")?.clone())
    } else {
        merge_runtime_configs(&codebases)?
    };

    let local_environment = load_local_dotenv(&codebases, &project_id, environment)?;
    let firebase_config = json!({
        "projectId": project_id,
        "storageBucket": format!("{project_id}.appspot.com"),
        "databaseURL": format!("http://{}?ns={project_id}", addresses.database),
    });
    Ok(FunctionsConfig {
        project_id,
        codebases,
        addresses,
        firebase_config,
        runtime_config,
        local_environment,
    })
}

/// Resolve the Pub/Sub listener without requiring a Functions source. This is
/// used by `--no-functions`, where firebase.json emulator settings still apply.
pub(crate) fn load_pubsub_address(
    root: Option<&Path>,
    cli: &ConfigOverrides,
    environment: &BTreeMap<String, String>,
) -> Result<SocketAddr, ConfigError> {
    let firebase = match root {
        Some(root) => read_optional_json(&root.join("firebase.json"))?.unwrap_or_else(|| json!({})),
        None => json!({}),
    };
    let explicit_host = cli
        .host
        .as_deref()
        .or_else(|| environment.get("FIREBASE_EMU_HOST").map(String::as_str));
    let host = if let Some(host) = explicit_host {
        parse_loopback_host(host, false)?
    } else if let Some(host) = firebase
        .pointer("/emulators/pubsub/host")
        .and_then(Value::as_str)
    {
        parse_loopback_host(host, true)?
    } else {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    };
    let port = cli.pubsub_port.unwrap_or(selected_port(
        environment,
        &firebase,
        "PUBSUB_EMULATOR_PORT",
        "pubsub",
        8085,
    )?);
    if port == 0 {
        return Err("Pub/Sub emulator port must be between 1 and 65535".into());
    }
    Ok(SocketAddr::new(host, port))
}

/// Resolve the loopback-only web UI address without requiring Functions.
pub(crate) fn load_ui_address(
    root: Option<&Path>,
    cli_port: Option<u16>,
    cli_host: Option<&str>,
    environment: &BTreeMap<String, String>,
) -> Result<SocketAddr, ConfigError> {
    let firebase = match root {
        Some(root) => read_optional_json(&root.join("firebase.json"))?.unwrap_or_else(|| json!({})),
        None => json!({}),
    };
    let host = if let Some(host) =
        cli_host.or_else(|| environment.get("FIREBASE_EMU_HOST").map(String::as_str))
    {
        parse_loopback_host(host, false)?
    } else if let Some(host) = firebase
        .pointer("/emulators/ui/host")
        .and_then(Value::as_str)
    {
        parse_loopback_host(host, true)?
    } else {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    };
    let file_port = firebase
        .pointer("/emulators/ui/port")
        .map(|value| {
            value
                .as_u64()
                .and_then(|port| u16::try_from(port).ok())
                .ok_or_else(|| -> ConfigError {
                    "firebase.json emulator `ui` port must be an integer between 0 and 65535".into()
                })
        })
        .transpose()?;
    let port = cli_port
        .or(env_port(environment, "FIREBASE_UI_EMU_PORT")?)
        .or(file_port)
        .unwrap_or(4000);
    Ok(SocketAddr::new(host, port))
}

fn read_optional_json(path: &Path) -> Result<Option<Value>, ConfigError> {
    match fs::read_to_string(path) {
        Ok(raw) => {
            Ok(Some(serde_json::from_str(&raw).map_err(|error| {
                format!("malformed {}: {error}", path.display())
            })?))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn validate_demo_project(project: &str) -> Result<(), ConfigError> {
    if !project.starts_with("demo-")
        || !project
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(
            format!("Functions requires a local `demo-` project ID, got `{project}`").into(),
        );
    }
    Ok(())
}

fn parse_loopback_host(host: &str, firebase_file: bool) -> Result<IpAddr, ConfigError> {
    let host = if host == "localhost" {
        "127.0.0.1"
    } else {
        host
    };
    let parsed: IpAddr = host
        .parse()
        .map_err(|_| format!("emulator host `{host}` must be an IP address or localhost"))?;
    if parsed.is_loopback() {
        Ok(parsed)
    } else if firebase_file && parsed.is_unspecified() {
        // Firebase projects commonly commit 0.0.0.0. Never inherit that unsafe bind.
        Ok(IpAddr::V4(Ipv4Addr::LOCALHOST))
    } else {
        Err(format!("emulator host `{host}` is not loopback").into())
    }
}

fn env_port(environment: &BTreeMap<String, String>, key: &str) -> Result<Option<u16>, ConfigError> {
    environment
        .get(key)
        .map(|raw| {
            raw.parse::<u16>()
                .map_err(|error| format!("invalid {key}: {error}").into())
        })
        .transpose()
}

fn json_port(firebase: &Value, service: &str) -> Result<Option<u16>, ConfigError> {
    let Some(value) = firebase.pointer(&format!("/emulators/{service}/port")) else {
        return Ok(None);
    };
    let port = value
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port != 0)
        .ok_or_else(|| {
            format!(
                "firebase.json emulator `{service}` port must be an integer between 1 and 65535"
            )
        })?;
    Ok(Some(port))
}

fn selected_port(
    environment: &BTreeMap<String, String>,
    firebase: &Value,
    key: &str,
    service: &str,
    default: u16,
) -> Result<u16, ConfigError> {
    let selected = env_port(environment, key)?
        .or(json_port(firebase, service)?)
        .unwrap_or(default);
    if selected == 0 {
        return Err(format!("{key} must be between 1 and 65535").into());
    }
    Ok(selected)
}

fn confined_source(root: &Path, source: &Path) -> Result<PathBuf, ConfigError> {
    if source.is_absolute() {
        return Err("Functions source must be relative to the Firebase project".into());
    }
    let resolved = root
        .join(source)
        .canonicalize()
        .map_err(|error| format!("invalid Functions source {}: {error}", source.display()))?;
    if !resolved.starts_with(root) {
        return Err(format!(
            "Functions source escapes project root: {}",
            source.display()
        )
        .into());
    }
    if !resolved.join("package.json").is_file() {
        return Err(format!(
            "Functions source has no package.json: {}",
            resolved.display()
        )
        .into());
    }
    Ok(resolved)
}

fn validate_codebase(codebase: &str) -> Result<(), ConfigError> {
    if codebase.is_empty()
        || !codebase
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!("invalid Functions codebase `{codebase}`").into());
    }
    Ok(())
}

fn resolve_runtime(configured: Option<&str>, source: &Path) -> Result<String, ConfigError> {
    let package = read_optional_json(&source.join("package.json"))?.unwrap_or_else(|| json!({}));
    let raw = configured
        .map(str::to_owned)
        .or_else(|| {
            package
                .pointer("/engines/node")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "20".into());
    let compact = raw.replace([' ', 'v'], "");
    for major in [18, 20, 22] {
        if compact == major.to_string()
            || compact == format!("{major}.x")
            || compact == format!("nodejs{major}")
            || compact.starts_with(&format!(">={major}<{}", major + 1))
        {
            return Ok(format!("nodejs{major}"));
        }
    }
    Err(format!("unsupported Functions Node runtime `{raw}`; expected 18, 20, or 22").into())
}

fn validate_object<'a>(
    value: &'a Value,
    label: &str,
) -> Result<&'a Map<String, Value>, ConfigError> {
    value
        .as_object()
        .ok_or_else(|| format!("{label} must be a JSON object").into())
}

fn merge_runtime_configs(codebases: &[FunctionCodebase]) -> Result<Value, ConfigError> {
    let mut result = Map::new();
    for codebase in codebases {
        let Some(value) = read_optional_json(&codebase.source.join(".runtimeconfig.json"))? else {
            continue;
        };
        let object = validate_object(&value, ".runtimeconfig.json")?;
        merge_objects(&mut result, object, "")?;
    }
    Ok(Value::Object(result))
}

fn merge_objects(
    target: &mut Map<String, Value>,
    source: &Map<String, Value>,
    prefix: &str,
) -> Result<(), ConfigError> {
    for (key, value) in source {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match (target.get_mut(key), value) {
            (Some(Value::Object(existing)), Value::Object(incoming)) => {
                merge_objects(existing, incoming, &path)?
            }
            (Some(existing), incoming) if existing != incoming => {
                return Err(
                    format!("conflicting runtime config at `{path}` across codebases").into(),
                )
            }
            (None, incoming) => {
                target.insert(key.clone(), incoming.clone());
            }
            _ => {}
        }
    }
    Ok(())
}

fn load_local_dotenv(
    codebases: &[FunctionCodebase],
    project: &str,
    environment: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut values = BTreeMap::new();
    for codebase in codebases {
        // Generic `.env` is intentionally excluded: an application checkout may
        // contain stage/production credentials. Demo-specific and `.env.local`
        // files are explicit local inputs and never leave this child process.
        for name in [format!(".env.{project}"), ".env.local".into()] {
            let path = codebase.source.join(name);
            if path.is_file() {
                for (key, value) in parse_dotenv(&fs::read_to_string(&path)?, &path)? {
                    values.insert(key, value);
                }
            }
        }
    }
    for (key, value) in values.clone() {
        if let Some(override_value) = environment.get(&key) {
            values.insert(key, override_value.clone());
        } else {
            values.insert(key, value);
        }
    }
    Ok(values)
}

fn parse_dotenv(raw: &str, path: &Path) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut result = BTreeMap::new();
    for (offset, original) in raw.lines().enumerate() {
        let mut line = original.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim_start();
        }
        let (key, raw_value) = line
            .split_once('=')
            .ok_or_else(|| format!("malformed {} line {}", path.display(), offset + 1))?;
        let key = key.trim();
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            || key.as_bytes()[0].is_ascii_digit()
        {
            return Err(format!(
                "invalid dotenv key in {} line {}",
                path.display(),
                offset + 1
            )
            .into());
        }
        let value = raw_value.trim();
        let value = if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            value[1..value.len() - 1].to_owned()
        } else {
            value
                .split(" #")
                .next()
                .unwrap_or(value)
                .trim_end()
                .to_owned()
        };
        result.insert(key.to_owned(), value);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let unique = format!(
                "firebase-emu-config-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            );
            let path = std::env::temp_dir().join(unique);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn write(&self, relative: &str, contents: &str) {
            let path = self.0.join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, contents).unwrap();
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn base() -> Fixture {
        let fixture = Fixture::new();
        fixture.write(
            "functions/package.json",
            r#"{"name":"fixture","engines":{"node":"22"}}"#,
        );
        fixture
    }

    #[test]
    fn precedence_is_cli_then_environment_then_files_then_defaults() {
        let fixture = base();
        fixture.write("firebase.json", r#"{"functions":{"source":"functions","runtime":"nodejs18"},"emulators":{"functions":{"host":"0.0.0.0","port":5101},"firestore":{"port":8180}}}"#);
        fixture.write(".firebaserc", r#"{"projects":{"default":"demo-file"}}"#);
        fixture.write(
            "functions/.runtimeconfig.json",
            r#"{"nested":{"from":"file","number":7}}"#,
        );
        fixture.write("functions/.env.demo-cli", "VALUE=file\nONLY_FILE=yes\n");
        fixture.write(
            "functions/.env.local",
            "VALUE=local\nQUOTED=\"nested value\"\n",
        );
        let env = BTreeMap::from([
            ("FIREBASE_EMU_PROJECT".into(), "demo-env".into()),
            ("FIREBASE_FUNCTIONS_EMU_PORT".into(), "5201".into()),
            ("FIREBASE_FUNCTIONS_RUNTIME".into(), "20".into()),
            ("VALUE".into(), "environment".into()),
            ("UNRELATED_SECRET".into(), "must-not-propagate".into()),
        ]);
        let cli = ConfigOverrides {
            project: Some("demo-cli".into()),
            functions_port: Some(5301),
            functions_runtime: Some("22".into()),
            runtime_config: Some(json!({"nested":{"from":"cli"}})),
            ..Default::default()
        };
        let loaded = load(&fixture.0, &cli, &env).unwrap();
        assert_eq!(loaded.project_id, "demo-cli");
        assert_eq!(loaded.addresses.functions.port(), 5301);
        assert!(loaded.addresses.functions.ip().is_loopback());
        assert_eq!(loaded.addresses.firestore.port(), 8180);
        assert_eq!(loaded.codebases[0].runtime, "nodejs22");
        assert_eq!(loaded.runtime_config, json!({"nested":{"from":"cli"}}));
        assert_eq!(loaded.local_environment["VALUE"], "environment");
        assert_eq!(loaded.local_environment["ONLY_FILE"], "yes");
        assert_eq!(loaded.local_environment["QUOTED"], "nested value");
        assert!(!loaded.local_environment.contains_key("UNRELATED_SECRET"));
    }

    #[test]
    fn discovers_multiple_codebases_and_nested_legacy_config() {
        let fixture = Fixture::new();
        fixture.write("firebase.json", r#"{"functions":[{"source":"one","codebase":"web"},{"source":"two","codebase":"uploads","runtime":"nodejs18"}],"emulators":{"auth":{"port":9190},"storage":{"port":9290},"database":{"port":9100},"pubsub":{"port":8185}}}"#);
        fixture.write(
            ".firebaserc",
            r#"{"projects":{"default":"local","local":"demo-multi"}}"#,
        );
        fixture.write("one/package.json", r#"{"engines":{"node":"22.x"}}"#);
        fixture.write("two/package.json", r#"{}"#);
        fixture.write(
            "one/.runtimeconfig.json",
            r#"{"service":{"url":"http://127.0.0.1","nested":{"enabled":true}}}"#,
        );
        fixture.write(
            "two/.runtimeconfig.json",
            r#"{"service":{"nested":{"limit":3}},"other":"value"}"#,
        );
        let loaded = load(&fixture.0, &ConfigOverrides::default(), &BTreeMap::new()).unwrap();
        assert_eq!(loaded.project_id, "demo-multi");
        assert_eq!(
            loaded
                .codebases
                .iter()
                .map(|item| (&*item.codebase, &*item.runtime))
                .collect::<Vec<_>>(),
            vec![("web", "nodejs22"), ("uploads", "nodejs18")]
        );
        assert_eq!(
            loaded.runtime_config,
            json!({"service":{"url":"http://127.0.0.1","nested":{"enabled":true,"limit":3}},"other":"value"})
        );
        assert_eq!(
            loaded.firebase_config["databaseURL"],
            "http://127.0.0.1:9100?ns=demo-multi"
        );
        assert_eq!(
            loaded.firebase_config["storageBucket"],
            "demo-multi.appspot.com"
        );
        let child = loaded.child_environment();
        assert_eq!(
            serde_json::from_str::<Value>(&child["CLOUD_RUNTIME_CONFIG"]).unwrap(),
            loaded.runtime_config
        );
        assert_eq!(
            serde_json::from_str::<Value>(&child["FIREBASE_CONFIG"]).unwrap(),
            loaded.firebase_config
        );
        assert_eq!(child["FIREBASE_AUTH_EMULATOR_HOST"], "127.0.0.1:9190");
        assert_eq!(child["PUBSUB_EMULATOR_HOST"], "127.0.0.1:8185");
    }

    #[test]
    fn pubsub_address_loads_without_a_functions_source() {
        let fixture = Fixture::new();
        fixture.write(
            "firebase.json",
            r#"{"emulators":{"pubsub":{"host":"localhost","port":8185}}}"#,
        );
        let loaded = load_pubsub_address(
            Some(&fixture.0),
            &ConfigOverrides::default(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(loaded.to_string(), "127.0.0.1:8185");
        let overridden = load_pubsub_address(
            Some(&fixture.0),
            &ConfigOverrides {
                pubsub_port: Some(8285),
                ..Default::default()
            },
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(overridden.to_string(), "127.0.0.1:8285");
    }

    #[test]
    fn ui_address_uses_firebase_config_and_explicit_override() {
        let fixture = Fixture::new();
        fixture.write(
            "firebase.json",
            r#"{"emulators":{"ui":{"host":"localhost","port":4400}}}"#,
        );
        let configured = load_ui_address(Some(&fixture.0), None, None, &BTreeMap::new()).unwrap();
        assert_eq!(configured, "127.0.0.1:4400".parse().unwrap());
        let overridden = load_ui_address(
            Some(&fixture.0),
            Some(4500),
            Some("127.0.0.1"),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(overridden, "127.0.0.1:4500".parse().unwrap());

        let dynamic = load_ui_address(
            Some(&fixture.0),
            Some(0),
            Some("127.0.0.1"),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(dynamic, "127.0.0.1:0".parse().unwrap());

        let dynamic_environment = load_ui_address(
            Some(&fixture.0),
            None,
            None,
            &BTreeMap::from([("FIREBASE_UI_EMU_PORT".into(), "0".into())]),
        )
        .unwrap();
        assert_eq!(dynamic_environment, "127.0.0.1:0".parse().unwrap());

        fixture.write(
            "firebase.json",
            r#"{"emulators":{"ui":{"host":"localhost","port":0}}}"#,
        );
        let dynamic_file = load_ui_address(Some(&fixture.0), None, None, &BTreeMap::new()).unwrap();
        assert_eq!(dynamic_file, "127.0.0.1:0".parse().unwrap());
    }

    #[test]
    fn environment_overrides_files_and_defaults_without_cli() {
        let fixture = base();
        fixture.write("firebase.json", r#"{"functions":{"source":"functions","runtime":"nodejs18"},"emulators":{"functions":{"port":5101}}}"#);
        fixture.write(".firebaserc", r#"{"projects":{"default":"demo-file"}}"#);
        let env = BTreeMap::from([
            ("FIREBASE_EMU_PROJECT".into(), "demo-env".into()),
            ("FIREBASE_FUNCTIONS_EMU_PORT".into(), "5201".into()),
            ("FIREBASE_FUNCTIONS_RUNTIME".into(), "20".into()),
            (
                "FIREBASE_EMU_RUNTIME_CONFIG".into(),
                r#"{"env":{"nested":"yes"}}"#.into(),
            ),
        ]);
        let loaded = load(&fixture.0, &ConfigOverrides::default(), &env).unwrap();
        assert_eq!(loaded.project_id, "demo-env");
        assert_eq!(loaded.addresses.functions.port(), 5201);
        assert_eq!(loaded.codebases[0].runtime, "nodejs20");
        assert_eq!(loaded.runtime_config, json!({"env":{"nested":"yes"}}));
    }

    #[test]
    fn rejects_production_projects_malformed_inputs_and_duplicate_codebases() {
        let fixture = base();
        fixture.write(".firebaserc", r#"{"projects":{"default":"example-prod"}}"#);
        assert!(
            load(&fixture.0, &ConfigOverrides::default(), &BTreeMap::new())
                .unwrap_err()
                .to_string()
                .contains("demo-")
        );

        fixture.write(".firebaserc", r#"{"projects":{"default":"demo-safe"}}"#);
        fixture.write("functions/.runtimeconfig.json", "[]");
        assert!(
            load(&fixture.0, &ConfigOverrides::default(), &BTreeMap::new())
                .unwrap_err()
                .to_string()
                .contains("JSON object")
        );
        fixture.write("functions/.runtimeconfig.json", "{}");
        fixture.write("functions/.env.local", "not valid\n");
        assert!(
            load(&fixture.0, &ConfigOverrides::default(), &BTreeMap::new())
                .unwrap_err()
                .to_string()
                .contains("malformed")
        );
        fs::remove_file(fixture.0.join("functions/.env.local")).unwrap();
        fixture.write("firebase.json", r#"{"functions":[{"source":"functions","codebase":"same"},{"source":"functions","codebase":"same"}]}"#);
        assert!(
            load(&fixture.0, &ConfigOverrides::default(), &BTreeMap::new())
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
    }

    #[test]
    fn rejects_escape_non_loopback_bad_runtime_and_malformed_json() {
        let fixture = base();
        fixture.write("firebase.json", r#"{"functions":{"source":"../outside"}}"#);
        assert!(load(
            &fixture.0,
            &ConfigOverrides {
                project: Some("demo-safe".into()),
                ..Default::default()
            },
            &BTreeMap::new()
        )
        .unwrap_err()
        .to_string()
        .contains("source"));
        fixture.write(
            "firebase.json",
            r#"{"functions":{"source":"functions","runtime":"nodejs16"}}"#,
        );
        assert!(load(
            &fixture.0,
            &ConfigOverrides {
                project: Some("demo-safe".into()),
                ..Default::default()
            },
            &BTreeMap::new()
        )
        .unwrap_err()
        .to_string()
        .contains("unsupported"));
        let env = BTreeMap::from([("FIREBASE_EMU_HOST".into(), "0.0.0.0".into())]);
        assert!(load(
            &fixture.0,
            &ConfigOverrides {
                project: Some("demo-safe".into()),
                ..Default::default()
            },
            &env
        )
        .unwrap_err()
        .to_string()
        .contains("not loopback"));
        fixture.write(
            "firebase.json",
            r#"{"functions":{"source":"functions"},"emulators":{"functions":{"port":70000}}}"#,
        );
        assert!(load(
            &fixture.0,
            &ConfigOverrides {
                project: Some("demo-safe".into()),
                ..Default::default()
            },
            &BTreeMap::new()
        )
        .unwrap_err()
        .to_string()
        .contains("port"));
        fixture.write("firebase.json", "{");
        assert!(load(
            &fixture.0,
            &ConfigOverrides {
                project: Some("demo-safe".into()),
                ..Default::default()
            },
            &BTreeMap::new()
        )
        .unwrap_err()
        .to_string()
        .contains("malformed"));
    }
}
