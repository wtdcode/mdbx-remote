use clap::Args;
use libmdbx_remote::{EnvironmentAny, Stat};
use tokio_stream::StreamExt;

use color_eyre::Result;

#[derive(Args)]
pub struct StatArguments {
    #[arg(index = 1)]
    pub url: String,

    #[arg(short, long)]
    pub all: bool,
}

fn print_stat(stat: &Stat) {
    println!("\tPage size: {}", stat.page_size());
    println!("\tEntries: {}", stat.entries());
    println!("\tLeaf Pages: {}", stat.leaf_pages());
    println!("\tBranch Pages: {}", stat.branch_pages());
    println!("\tDepth: {}", stat.depth());
    println!("\tOverflow Pages: {}", stat.overflow_pages());
}

pub async fn stat_main(args: StatArguments) -> Result<()> {
    let db = EnvironmentAny::open(&args.url).await?;
    let stat = db.stat().await?;

    println!("Database:");
    print_stat(&stat);

    let tx = db.begin_ro_txn().await?;
    let db = tx.open_db(None).await?;
    let stat = tx.db_stat(&db).await?;
    println!("Table Main:");
    print_stat(&stat);
    if args.all {
        let mut cur = tx.cursor(&db).await?;

        let mut st = cur.iter::<Vec<u8>, ()>();
        while let Some(it) = st.next().await {
            let (k, _) = it?;
            let table = String::from_utf8(k)?;
            let db = tx.open_db(Some(&table)).await?;

            let stat = tx.db_stat(&db).await?;
            println!("Table {}:", table);
            print_stat(&stat);
        }
    }
    Ok(())
}
