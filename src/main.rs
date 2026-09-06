mod auth;
mod firestore;
mod firestore_web;
mod functions;
mod functions_config;
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
            "--project" => result.functions.project = Some(option_value(&mut args, "--project")?),
            "--host" => result.functions.host = Some(option_value(&mut args, "--host")?),
            "--functions-port" => {
                result.functions.functions_port =
                    Some(option_value(&mut args, "--functions-port")?.parse()?)
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
                println!("firebase-emu [--config DIR] [--project demo-ID] [--host LOOPBACK] [--functions-port PORT] [--functions-source DIR] [--functions-codebase NAME] [--functions-runtime nodejs18|nodejs20|nodejs22] [--runtime-config JSON] [--no-functions]");
                std::process::exit(0);
            }
            _ => return Err(format!("unknown option `{argument}` (use --help)").into()),
        }
    }
    Ok(result)
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
async fn main() -> Result<(), BoxError> {
    let cli = command_line()?;
    let functions_config = if cli.no_functions {
        None
    } else if let Some(root) = configured_root(&cli)? {
        Some(functions_config::load(
            &root,
            &cli.functions,
            &environment_snapshot(),
        )?)
    } else {
        None
    };
    let (firestore_addr, auth_addr, storage_addr) = if let Some(config) = &functions_config {
        (
            config.addresses.firestore,
            config.addresses.auth,
            config.addresses.storage,
        )
    } else {
        (
            address("FIRESTORE_EMU_PORT", 8080)?,
            address("FIREBASE_AUTH_EMU_PORT", 9099)?,
            address("FIREBASE_STORAGE_EMU_PORT", 9199)?,
        )
    };
    let auth_project = functions_config
        .as_ref()
        .map(|config| config.project_id.as_str())
        .or(cli.functions.project.as_deref());
    let auth_router = auth::router_for_project(auth_project);

    eprintln!("Firestore emulator listening on {firestore_addr}");
    eprintln!("Auth emulator listening on {auth_addr}");
    eprintln!("Storage emulator listening on {storage_addr}");

    if let Some(config) = functions_config {
        // Every service is long-lived. A clean Functions shutdown or any service
        // failure ends the process and drops the remaining listener futures.
        tokio::select! {
            result = firestore::serve(firestore_addr) => result,
            result = serve_http(auth_addr, auth_router) => result,
            result = serve_http(storage_addr, storage::router()) => result,
            result = functions::serve(config) => result,
        }
    } else {
        tokio::try_join!(
            firestore::serve(firestore_addr),
            serve_http(auth_addr, auth_router),
            serve_http(storage_addr, storage::router()),
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
}
