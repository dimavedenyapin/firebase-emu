mod auth;
mod firestore;
mod firestore_web;
mod functions;
mod functions_config;
mod persistence;
mod pubsub;
mod storage;
mod ui;

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
    serve_http_listener(listener, router).await
}

async fn serve_http_listener(
    listener: tokio::net::TcpListener,
    router: axum::Router,
) -> Result<(), BoxError> {
    axum::serve(listener, router).await?;
    Ok(())
}

async fn bind_ui_listener(addr: SocketAddr) -> Result<tokio::net::TcpListener, BoxError> {
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => Ok(listener),
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse && addr.port() != 0 => {
            let listener = tokio::net::TcpListener::bind(SocketAddr::new(addr.ip(), 0)).await?;
            eprintln!(
                "Emulator UI port {} is in use; selected free port {}",
                addr.port(),
                listener.local_addr()?.port()
            );
            Ok(listener)
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Default)]
struct CommandLine {
    config_root: Option<PathBuf>,
    data_dir: Option<PathBuf>,
    in_memory: bool,
    no_functions: bool,
    no_ui: bool,
    ui_port: Option<u16>,
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
            "--ui-port" => result.ui_port = Some(option_value(&mut args, "--ui-port")?.parse()?),
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
            "--no-ui" => result.no_ui = true,
            "--help" | "-h" => {
                println!("firebase-emu [--data-dir PATH | --in-memory] [--config DIR] [--project demo-ID] [--host LOOPBACK] [--functions-port PORT] [--pubsub-port PORT] [--ui-port PORT] [--functions-source DIR] [--functions-codebase NAME] [--functions-runtime nodejs18|nodejs20|nodejs22] [--runtime-config JSON] [--no-functions] [--no-ui]");
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
    let default_project = auth_project
        .map(str::to_owned)
        .or_else(|| environment.get("GCLOUD_PROJECT").cloned())
        .or_else(|| environment.get("GOOGLE_CLOUD_PROJECT").cloned())
        .unwrap_or_else(|| "demo-rust-emu".into());
    let ui_addr = functions_config::load_ui_address(
        config_root.as_deref(),
        cli.ui_port,
        cli.functions.host.as_deref(),
        &environment,
    )?;
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
    let firestore = firestore::FirestoreService::new(persistence.clone());
    let ui_listener = if cli.no_ui {
        None
    } else {
        Some(bind_ui_listener(ui_addr).await?)
    };

    eprintln!("Firestore emulator listening on {firestore_addr}");
    eprintln!("Auth emulator listening on {auth_addr}");
    eprintln!("Storage emulator listening on {storage_addr}");
    eprintln!("Pub/Sub emulator listening on {pubsub_addr}");
    if let Some(listener) = &ui_listener {
        eprintln!("Emulator UI listening on http://{}", listener.local_addr()?);
    }
    eprintln!("SDK connection: PUBSUB_EMULATOR_HOST={pubsub_addr}");

    let ui_router = ui::router(ui::UiState::new(
        default_project,
        auth_addr,
        firestore.clone(),
        pubsub.clone(),
    ));
    if let Some(config) = functions_config {
        // Every service is long-lived. A clean Functions shutdown or any service
        // failure ends the process and drops the remaining listener futures.
        if let Some(listener) = ui_listener {
            tokio::select! {
                result = firestore::serve(firestore_addr, firestore.clone()) => result,
                result = serve_http(auth_addr, auth_router) => result,
                result = serve_http(storage_addr, storage_router) => result,
                result = pubsub::serve(pubsub_addr, pubsub.clone()) => result,
                result = functions::serve(config, persistence.clone(), pubsub) => result,
                result = serve_http_listener(listener, ui_router) => result,
            }
        } else {
            tokio::select! {
                result = firestore::serve(firestore_addr, firestore.clone()) => result,
                result = serve_http(auth_addr, auth_router) => result,
                result = serve_http(storage_addr, storage_router) => result,
                result = pubsub::serve(pubsub_addr, pubsub.clone()) => result,
                result = functions::serve(config, persistence.clone(), pubsub) => result,
            }
        }
    } else {
        if cli.no_ui {
            tokio::try_join!(
                firestore::serve(firestore_addr, firestore),
                serve_http(auth_addr, auth_router),
                serve_http(storage_addr, storage_router),
                pubsub::serve(pubsub_addr, pubsub),
            )?;
        } else {
            tokio::try_join!(
                firestore::serve(firestore_addr, firestore),
                serve_http(auth_addr, auth_router),
                serve_http(storage_addr, storage_router),
                pubsub::serve(pubsub_addr, pubsub),
                serve_http_listener(ui_listener.expect("UI listener is bound"), ui_router),
            )?;
        }
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

    #[tokio::test]
    async fn ui_listener_uses_dynamic_port_and_falls_back_from_occupied_port() {
        let host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let dynamic = bind_ui_listener(SocketAddr::new(host, 0)).await.unwrap();
        assert_ne!(dynamic.local_addr().unwrap().port(), 0);

        let occupied = tokio::net::TcpListener::bind(SocketAddr::new(host, 0))
            .await
            .unwrap();
        let requested = occupied.local_addr().unwrap();
        let fallback = bind_ui_listener(requested).await.unwrap();
        assert_ne!(fallback.local_addr().unwrap().port(), requested.port());
        assert_eq!(fallback.local_addr().unwrap().ip(), requested.ip());
    }
}
