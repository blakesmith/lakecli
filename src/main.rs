use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Files {
        table: String,
    },
    History {
        table: String,

        #[clap(long, default_value = None)]
        limit: Option<usize>,
    },
    Metadata {
        table: String,
    },
    Schema {
        table: String,
    },
    Version {
        table: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Files { table }) => {
            let table = deltalake::open_table(&table).await?;
            let files: Vec<_> = table.get_file_uris()?.collect();
            println!("files: {:?}", files);
        }
        Some(Commands::Schema { table }) => {
            let table = deltalake::open_table(&table).await?;
            println!("schema: {:?}", table.schema());
        }
        Some(Commands::Version { table }) => {
            let table = deltalake::open_table(&table).await?;
            println!("version: {}", table.version());
        }
        Some(Commands::Metadata { table }) => {
            let table = deltalake::open_table(&table).await?;
            println!("metadata: {:?}", table.metadata()?);
        }
        Some(Commands::History { table, limit }) => {
            let table = deltalake::open_table(&table).await?;
            let history = table.history(limit.clone()).await?;
            println!("history:");
            for commit in history {
                println!("{:?}", commit);
            }
        }
        None => {
            println!("No command");
        }
    }

    Ok(())
}