use clap::Args;
use color_eyre::Result;
use libmdbx_remote::EnvironmentAny;
use reth_db::table::Decompress;
use reth_db::{table::Table, PlainAccountState};

fn decode_key(s: &str) -> Result<Vec<u8>> {
    Ok(alloy::hex::decode(s)?)
}

#[derive(Args)]
pub struct RethArguments {
    #[arg(short, long)]
    pub url: String,

    #[arg(short, long)]
    pub table: String,

    #[arg(index = 1, value_parser=decode_key)]
    pub key: std::vec::Vec<u8>,
}

pub async fn reth_main(args: RethArguments) -> Result<()> {
    let db = EnvironmentAny::open(&args.url).await?;
    let tx = db.begin_ro_txn().await?;

    match args.table.to_lowercase().as_str() {
        "plainaccountstate" => {
            let db = tx.open_db(Some(PlainAccountState::NAME)).await?;
            let val = tx.get::<Vec<u8>>(db.dbi(), &args.key).await?;
            let val = val
                .map(|t| <PlainAccountState as reth_db::table::Table>::Value::decompress(&t))
                .transpose()?;
            if let Some(val) = val {
                println!("The account is:\n{}", serde_json::to_string_pretty(&val)?);
            } else {
                println!("No such key");
            }
        }
        _ => {
            println!("Unsupported table");
        }
    }

    Ok(())
}
