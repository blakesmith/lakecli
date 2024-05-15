use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[clap(about = "List files in the table")]
    Files { table: String },
    #[clap(about = "Show table history")]
    History {
        table: String,

        #[clap(long, default_value = None)]
        limit: Option<usize>,
    },
    #[clap(about = "Print table metadata")]
    Metadata { table: String },

    #[clap(about = "Show table schema")]
    Schema { table: String },

    #[clap(about = "Print the current / latest table version number")]
    Version { table: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Files { table } => {
            let table = deltalake::open_table(&table).await?;
            let files: Vec<_> = table.get_file_uris()?.collect();
            println!("files: {:?}", files);
        }
        Commands::Schema { table } => {
            let table = deltalake::open_table(&table).await?;
            println!("schema: {:?}", table.schema());
        }
        Commands::Version { table } => {
            let table = deltalake::open_table(&table).await?;
            println!("version: {}", table.version());
        }
        Commands::Metadata { table } => {
            let table = deltalake::open_table(&table).await?;
            println!("metadata: {:?}", table.metadata()?);
        }
        Commands::History { table, limit } => {
            let table = deltalake::open_table(&table).await?;
            let history = table.history(limit.clone()).await?;
            println!("history:");
            for commit in history {
                println!("{:?}", commit);
            }
        }
    }

    Ok(())
}
