use clap::Args;

use color_eyre::Result;
use libmdbx_remote::{BufferConfiguration, EnvironmentAny};
use tokio_stream::StreamExt;

#[derive(Args)]
pub struct DumpArguments {
    #[arg(index = 1)]
    pub input: String,

    #[arg(short, long, default_value_t = 128)]
    pub batch: u64,

    #[arg(short, long)]
    pub table: Option<String>,
}

pub async fn mdbx_dump(args: DumpArguments) -> Result<()> {
    let src = EnvironmentAny::open(&args.input).await?;

    let src_tx = src.begin_ro_txn().await?;
    let src_main_db = src_tx.open_db(None).await?;
    let mut src_db_cur = src_tx.cursor(&src_main_db).await?;

    let mut st = src_db_cur.iter::<Vec<u8>, ()>();
    while let Some(it) = st.next().await {
        let (k, _) = it?;
        let table = String::from_utf8(k)?;

        if let Some(request) = &args.table {
            if request != &table {
                continue;
            }
        }

        println!("Dumping table {}\n===============\n", &table);
        let table = src_tx.open_db(Some(table.as_str())).await?;
        let table_cur = src_tx.cursor(&table).await?;

        let mut st2 = table_cur.into_iter_buffered::<Vec<u8>, Vec<u8>>(
            BufferConfiguration::default().max_count(args.batch),
        );
        while let Some(it2) = st2.next().await {
            let (k, v) = it2?;

            println!("{}\t{}", alloy::hex::encode(&k), alloy::hex::encode(&v));
        }
    }

    Ok(())
}
