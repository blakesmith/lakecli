use clap::{Parser, Subcommand};
use deltalake::{datafusion::prelude::SessionContext, kernel::StructType};
use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn print_schema(schema: &StructType) {
    println!("{0: <20} | {1: <8} | {2: <10}", "name", "type", "nullable");
    println!("{0:-<20} + {1:-<8} + {2:-<10}", "", "", "");
    for field in &schema.fields {
        let data_type = format!("{}", field.data_type);
        println!(
            "{0: <20} | {1: <8} | {2: <10}",
            field.name, data_type, field.nullable
        );
    }
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

    #[clap(about = "Query the table with a DataFusion query")]
    Query { table: String, query: String },
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
            match table.schema() {
                Some(schema) => print_schema(schema),
                None => {
                    println!("No schema found in delta table!");
                }
            }
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
        Commands::Query { table, query } => {
            println!("Executing query: {}", query);
            let table = Arc::new(deltalake::open_table(&table).await?);
            let ctx = SessionContext::new();
            let metadata = table.metadata()?;
            if let Some(table_name) = &metadata.name {
                ctx.register_table(table_name, table.clone())
                    .expect("Failed to register table");
            }
            // Always register table 't', for simplicity
            ctx.register_table("t", table.clone())
                .expect("Failed to register table");
            let dataframe = ctx.sql(&query).await?;
            dataframe.show().await?
        }
    }

    Ok(())
}
