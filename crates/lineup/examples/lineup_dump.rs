//! `lineup_dump`: the parity harness's view of one lineup file.
//!
//! Loads one embedded lineup exactly as `sync` will (prefix rewrite, detail fold,
//! country resolution, group names, numbers) and prints one JSON object to stdout:
//! the file metadata, the folded categories in file order, and every entry in file
//! order. `scripts/lineup_file_parity.py` diffs this against Lineuparr's own
//! `_load_lineup` pipeline entry for entry.

#[derive(Debug)]
struct Args {
    file: String,
    detail: String,
    prefix: String,
}

fn parse_args() -> Result<Args, String> {
    let mut file = None;
    let mut detail = None;
    let mut prefix = None;
    let rest: Vec<String> = std::env::args().skip(1).collect();
    // `--flag value` and `--flag=value` both work; `--prefix` may be empty or absent.
    let mut i = 0;
    while i < rest.len() {
        let arg = rest[i].clone();
        let (key, inline) = match arg.split_once('=') {
            Some((key, value)) => (key.to_string(), Some(value.to_string())),
            None => (arg, None),
        };
        let value = |inline: Option<String>, i: &mut usize| -> Result<String, String> {
            if let Some(value) = inline {
                return Ok(value);
            }
            *i += 1;
            rest.get(*i)
                .cloned()
                .ok_or_else(|| format!("{key} needs a value"))
        };
        match key.as_str() {
            "--file" => file = Some(value(inline, &mut i)?),
            "--detail" => detail = Some(value(inline, &mut i)?),
            "--prefix" => prefix = Some(value(inline, &mut i)?),
            other => return Err(format!("unknown argument {other:?}")),
        }
        i += 1;
    }
    Ok(Args {
        file: file.ok_or_else(|| "--file is required".to_string())?,
        detail: detail.unwrap_or_else(|| "normal".to_string()),
        prefix: prefix.unwrap_or_default(),
    })
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("error: {error}");
            eprintln!("usage: lineup_dump --file <name> [--detail <mode>] [--prefix <prefix>]");
            std::process::exit(2);
        }
    };
    match run(&args) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("error: {error:#}");
            std::process::exit(1);
        }
    }
}

fn run(args: &Args) -> anyhow::Result<()> {
    let lineup =
        crosstalk_lineup::lineup_file::load_lineup(&args.file, &args.detail, &args.prefix)?;
    let entries: Vec<serde_json::Value> = lineup
        .entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "name": entry.name,
                "number": entry.number,
                "category": entry.category,
                "group": entry.group,
                "country": entry.country,
                "aliases": entry.aliases,
            })
        })
        .collect();
    println!(
        "{}",
        serde_json::json!({
            "file": lineup.file_name,
            "package": lineup.package,
            "country": lineup.country,
            "prefix": lineup.prefix,
            "detail": lineup.detail,
            "categories": lineup.categories,
            "entries": entries,
        })
    );
    Ok(())
}
