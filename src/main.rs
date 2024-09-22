use clap::{Parser, Subcommand};
use deltalake::{
    datafusion::{
        common::DFSchema, dataframe::DataFrame, error::DataFusionError, prelude::SessionContext,
    },
    kernel::StructType,
    DeltaTable, DeltaTableError,
};
use std::{ffi::OsStr, path::Path, sync::Arc};
use thiserror::Error;

#[derive(Parser)]
#[command(author, version, about, long_about)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn print_delta_schema(schema: &StructType) {
    println!("{0: <20} | {1: <8} | {2: <10}", "name", "type", "nullable");
    println!("{0:-<20} + {1:-<8} + {2:-<10}", "", "", "");
    for field in schema.fields() {
        let data_type = format!("{}", field.data_type);
        println!(
            "{0: <20} | {1: <8} | {2: <10}",
            field.name, data_type, field.nullable
        );
    }
}

fn print_dataframe_schema(schema: &DFSchema) {
    println!("{0: <20} | {1: <8} | {2: <10}", "name", "type", "nullable");
    println!("{0:-<20} + {1:-<8} + {2:-<10}", "", "", "");
    for field in schema.fields() {
        let data_type = format!("{}", field.data_type());
        println!(
            "{0: <20} | {1: <8} | {2: <10}",
            field.name(),
            data_type,
            field.is_nullable()
        );
    }
}

#[derive(Debug)]
enum Table {
    Delta(Arc<DeltaTable>),
    Parquet { table_path: Box<str> },
}

impl Table {
    pub async fn files(&self) -> Result<(), Error> {
        match self {
            Table::Delta(delta_table) => {
                let files: Vec<_> = delta_table.get_file_uris()?.collect();
                println!("files: {:?}", files);
            }
            Table::Parquet { table_path } => {
                let files = vec![table_path];
                println!("files: {:?}", files);
            }
        }

        Ok(())
    }

    pub async fn schema(&self, ctx: &SessionContext) -> Result<(), Error> {
        match self {
            Table::Delta(delta_table) => match delta_table.schema() {
                Some(schema) => print_delta_schema(schema),
                None => {
                    println!("No schema found in delta table!");
                }
            },
            Table::Parquet { table_path: _ } => {
                let dataframe = self.register_table(ctx).await?;
                print_dataframe_schema(dataframe.schema());
            }
        }

        Ok(())
    }

    pub async fn version(&self) -> Result<(), Error> {
        match self {
            Table::Delta(delta_table) => {
                println!("version: {}", delta_table.version());
            }
            other => {
                println!("'version' call unsupported for: {:?}", other);
            }
        }

        Ok(())
    }

    pub async fn metadata(&self, ctx: &SessionContext) -> Result<(), Error> {
        match self {
            Table::Delta(delta_table) => {
                println!("metadata: {:?}", delta_table.metadata()?);
            }
            Table::Parquet { table_path: _ } => {
                let dataframe = self.register_table(ctx).await?;
                println!("metadata: {:?}", dataframe.schema().metadata());
            }
        }

        Ok(())
    }

    pub async fn history(&self, limit: Option<usize>) -> Result<(), Error> {
        match self {
            Table::Delta(delta_table) => {
                let history = delta_table.history(limit.clone()).await?;
                println!("history:");
                for commit in history {
                    println!("{:?}", commit);
                }
            }
            other => {
                println!("'history' call unsupported for: {:?}", other);
            }
        }

        Ok(())
    }

    pub async fn query(&self, ctx: &SessionContext, query: &str) -> Result<(), Error> {
        self.register_table(ctx).await?;
        let dataframe = ctx.sql(&query).await?;
        dataframe.show().await?;
        Ok(())
    }

    async fn register_table(&self, ctx: &SessionContext) -> Result<DataFrame, Error> {
        match self {
            Table::Delta(delta_table) => {
                let metadata = delta_table.metadata()?;
                if let Some(table_name) = &metadata.name {
                    ctx.register_table(table_name, delta_table.clone())?;
                }
                // Always register table 't', for simplicity
                ctx.register_table("t", delta_table.clone())?;
                ctx.table("t").await.map_err(|err| err.into())
            }
            Table::Parquet { table_path } => {
                ctx.register_parquet("t", table_path, Default::default())
                    .await?;
                ctx.table("t").await.map_err(|err| err.into())
            }
        }
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("Unknown table format")]
    UnknownTableFormat,

    #[error("Delta table error")]
    Delta(#[from] DeltaTableError),

    #[error("Data fusion error")]
    DataFusion(#[from] DataFusionError),
}

async fn open_table(table_name: &str) -> Result<Table, Error> {
    let extension = Path::new(table_name).extension().and_then(OsStr::to_str);
    match extension {
        Some("parquet") => Ok(Table::Parquet {
            table_path: table_name.into(),
        }),
        Some(_other) => Err(Error::UnknownTableFormat),
        None => {
            let table =
                deltalake::open_table_with_storage_options(table_name, Default::default()).await?;
            Ok(Table::Delta(Arc::new(table)))
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    #[clap(about = "List files in the table")]
    Files { table: String },
    #[clap(about = "Show table history. Currently for delta tables only")]
    History {
        table: String,

        #[clap(long, default_value = None)]
        limit: Option<usize>,
    },
    #[clap(about = "Print table metadata")]
    Metadata { table: String },

    #[clap(about = "Show table schema")]
    Schema { table: String },

    #[clap(
        about = "Print the current / latest table version number. Currently for delta tables only"
    )]
    Version { table: String },

    #[clap(about = "Query the table with a DataFusion query")]
    Query { table: String, query: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    deltalake::aws::register_handlers(None);
    let ctx = SessionContext::new();

    match &cli.command {
        Commands::Files { table } => {
            let table = open_table(&table).await?;
            table.files().await?;
        }
        Commands::Schema { table } => {
            let table = open_table(&table).await?;
            table.schema(&ctx).await?;
        }
        Commands::Version { table } => {
            let table = open_table(&table).await?;
            table.version().await?;
        }
        Commands::Metadata { table } => {
            let table = open_table(&table).await?;
            table.metadata(&ctx).await?;
        }
        Commands::History { table, limit } => {
            let table = open_table(&table).await?;
            table.history(*limit).await?;
        }
        Commands::Query { table, query } => {
            let table_name = table;
            let table = open_table(&table_name).await?;
            table.query(&ctx, query).await?;
        }
    }

    Ok(())
}
