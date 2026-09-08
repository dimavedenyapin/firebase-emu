mod auth;
mod firestore;
mod firestore_web;
mod functions;
mod functions_config;
mod persistence;
mod pubsub;
mod storage;

use std::{
    collections::BTreeMap,
    env,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn address(port_variable: &str, default_port: u16) -> Result<SocketAddr, BoxError> {
    let host = env::var("FIREBASE_EMU_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let ip: IpAddr = host.parse()?;
    if !ip.is_loopback() {
        return Err("FIREBASE_EMU_HOST must be a loopback IP address".into());
    }
    let port = env::var(port_variable)
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(default_port);
    Ok(SocketAddr::new(ip, port))
}

async fn serve_http(addr: SocketAddr, router: axum::Router) -> Result<(), BoxError> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

#[derive(Default)]
struct CommandLine {
    config_root: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    in_memory: bool,
    no_functions: bool,
    functions: functions_config::ConfigOverrides,
}

fn option_value(
    args: &mut impl Iterator<Item = std::ffi::OsString>,
    option: &str,
) -> Result<String, BoxError> {
    args.next()
        .ok_or_else(|| -> BoxError { format!("{option} requires a value").into() })?
        .into_string()
        .map_err(|_| format!("{option} must be valid UTF-8").into())
}

fn command_line() -> Result<CommandLine, BoxError> {
    let mut result = CommandLine::default();
    let mut args = env::args_os().skip(1);
    while let Some(argument) = args.next() {
        let argument = argument
            .into_string()
            .map_err(|_| "command-line options must be valid UTF-8")?;
        match argument.as_str() {
            "--config" => {
                result.config_root = Some(PathBuf::from(option_value(&mut args, "--config")?))
            }
            "--data-dir" => {
                result.data_dir = Some(PathBuf::from(option_value(&mut args, "--data-dir")?))
            }
            "--in-memory" => result.in_memory = true,
            "--project" => result.functions.project = Some(option_value(&mut args, "--project")?),
            "--host" => result.functions.host = Some(option_value(&mut args, "--host")?),
            "--functions-port" => {
                result.functions.functions_port =
                    Some(option_value(&mut args, "--functions-port")?.parse()?)
            }
            "--pubsub-port" => {
                result.functions.pubsub_port =
                    Some(option_value(&mut args, "--pubsub-port")?.parse()?)
            }
            "--functions-source" => {
                result.functions.functions_source = Some(PathBuf::from(option_value(
                    &mut args,
                    "--functions-source",
                )?))
            }
            "--functions-codebase" => {
                result.functions.functions_codebase =
                    Some(option_value(&mut args, "--functions-codebase")?)
            }
            "--functions-runtime" => {
                result.functions.functions_runtime =
                    Some(option_value(&mut args, "--functions-runtime")?)
            }
            "--runtime-config" => {
                let raw = option_value(&mut args, "--runtime-config")?;
                result.functions.runtime_config = Some(
                    serde_json::from_str(&raw)
                        .map_err(|error| format!("malformed --runtime-config JSON: {error}"))?,
                );
            }
            "--no-functions" => result.no_functions = true,
            "--help" | "-h" => {
                println!("firebase-emu [--data-dir PATH | --in-memory] [--config DIR] [--project demo-ID] [--host LOOPBACK] [--functions-port PORT] [--pubsub-port PORT] [--functions-source DIR] [--functions-codebase NAME] [--functions-runtime nodejs18|nodejs20|nodejs22] [--runtime-config JSON] [--no-functions]");
                std::process::exit(0);
            }
            _ => return Err(format!("unknown option `{argument}` (use --help)").into()),
        }
    }
    validate_command_line(&result)?;
    Ok(result)
}

fn validate_command_line(result: &CommandLine) -> Result<(), BoxError> {
    if result.data_dir.is_some() && result.in_memory {
        return Err("--data-dir and --in-memory cannot be used together".into());
    }
    Ok(())
}

fn environment_snapshot() -> BTreeMap<String, String> {
    env::vars().collect()
}

fn configured_root(cli: &CommandLine) -> Result<Option<PathBuf>, BoxError> {
    let explicit = cli
        .config_root
        .clone()
        .or_else(|| env::var_os("FIREBASE_EMU_CONFIG_DIR").map(PathBuf::from));
    let mut root = if let Some(path) = explicit {
        Some(path)
    } else if cli.functions.functions_source.is_some()
        || env::var_os("FIREBASE_FUNCTIONS_SOURCE").is_some()
    {
        Some(env::current_dir()?)
    } else {
        let current = env::current_dir()?;
        current.join("firebase.json").is_file().then_some(current)
    };
    if let Some(path) = root.as_mut() {
        if path.file_name().and_then(|name| name.to_str()) == Some("firebase.json") {
            *path = path
                .parent()
                .ok_or("firebase.json must have a parent directory")?
                .to_owned();
        }
    }
    Ok(root)
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("firebase-emu: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), BoxError> {
    let cli = command_line()?;
    let config_root = configured_root(&cli)?;
    let environment = environment_snapshot();
    let functions_config = if cli.no_functions {
        None
    } else if let Some(root) = &config_root {
        Some(functions_config::load(root, &cli.functions, &environment)?)
    } else {
        None
    };
    let (firestore_addr, auth_addr, storage_addr, pubsub_addr) =
        if let Some(config) = &functions_config {
            (
                config.addresses.firestore,
                config.addresses.auth,
                config.addresses.storage,
                config.addresses.pubsub,
            )
        } else {
            (
                address("FIRESTORE_EMU_PORT", 8080)?,
                address("FIREBASE_AUTH_EMU_PORT", 9099)?,
                address("FIREBASE_STORAGE_EMU_PORT", 9199)?,
                functions_config::load_pubsub_address(
                    config_root.as_deref(),
                    &cli.functions,
                    &environment,
                )?,
            )
        };
    let auth_project = functions_config
        .as_ref()
        .map(|config| config.project_id.as_str())
        .or(cli.functions.project.as_deref());
    let persistence = match cli.data_dir.clone() {
        Some(path) => {
            let path = if path.is_absolute() {
                path
            } else {
                env::current_dir()?.join(path)
            };
            let persistence =
                persistence::Persistence::open(path, functions_config.is_some()).await?;
            eprintln!(
                "Persistence: SQLite WAL at {} (objects: {})",
                persistence.database_path().display(),
                persistence.blobs_dir().display()
            );
            Some(persistence)
        }
        None => {
            eprintln!("Persistence: in-memory (data is discarded on exit)");
            None
        }
    };
    let auth_router = auth::router_for_project(auth_project, persistence.clone());
    let storage_router = storage::router(persistence.clone()).await?;
    let pubsub = pubsub::PubSubService::new(persistence.clone());

    eprintln!("Firestore emulator listening on {firestore_addr}");
    eprintln!("Auth emulator listening on {auth_addr}");
    eprintln!("Storage emulator listening on {storage_addr}");
    eprintln!("Pub/Sub emulator listening on {pubsub_addr}");
    eprintln!("SDK connection: PUBSUB_EMULATOR_HOST={pubsub_addr}");

    if let Some(config) = functions_config {
        // Every service is long-lived. A clean Functions shutdown or any service
        // failure ends the process and drops the remaining listener futures.
        tokio::select! {
            result = firestore::serve(firestore_addr, persistence.clone()) => result,
            result = serve_http(auth_addr, auth_router) => result,
            result = serve_http(storage_addr, storage_router) => result,
            result = pubsub::serve(pubsub_addr, pubsub.clone()) => result,
            result = functions::serve(config, persistence.clone(), pubsub) => result,
        }
    } else {
        tokio::try_join!(
            firestore::serve(firestore_addr, persistence.clone()),
            serve_http(auth_addr, auth_router),
            serve_http(storage_addr, storage_router),
            pubsub::serve(pubsub_addr, pubsub),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn address_defaults_to_loopback() {
        std::env::remove_var("FIREBASE_EMU_HOST");
        std::env::remove_var("FIRESTORE_EMU_PORT");
        assert_eq!(
            address("FIRESTORE_EMU_PORT", 8080).unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)
        );
    }

    #[test]
    fn persistence_flags_are_mutually_exclusive() {
        let command = CommandLine {
            data_dir: Some(PathBuf::from("state")),
            in_memory: true,
            ..CommandLine::default()
        };
        assert!(validate_command_line(&command)
            .unwrap_err()
            .to_string()
            .contains("cannot be used together"));
    }
}
