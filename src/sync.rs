use crate::database::Database;
use hbb_common::{log, ResultType};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, Row};
use std::str::FromStr;
use std::time::Duration;

pub async fn run_sync_loop(db: Database, api_db_path: String) {
    let mut interval = hbb_common::tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        if let Err(err) = sync_once(&db, &api_db_path).await {
            log::error!("sync api db failed: {}", err);
        }
    }
}

async fn sync_once(db: &Database, api_db_path: &str) -> ResultType<()> {
    if !std::path::Path::new(api_db_path).exists() {
        log::warn!("api db not found: {}", api_db_path);
        return Ok(());
    }
    let url = format!("sqlite://{}", api_db_path);
    let opt = SqliteConnectOptions::from_str(&url)?
        .read_only(true)
        .journal_mode(SqliteJournalMode::Delete)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));
    let mut conn = sqlx::SqliteConnection::connect_with(&opt).await?;

    let peer_rows = sqlx::query("select id, lease_expire_at from peers")
        .fetch_all(&mut conn)
        .await?;
    let peers: Vec<(String, i64)> = peer_rows
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("id"),
                r.get::<i64, _>("lease_expire_at"),
            )
        })
        .collect();

    let user_rows = sqlx::query("select id, is_admin, status from users")
        .fetch_all(&mut conn)
        .await?;
    let users: Vec<(i64, i64, i64)> = user_rows
        .iter()
        .map(|r| {
            (
                r.get::<i64, _>("id"),
                r.get::<i64, _>("is_admin"),
                r.get::<i64, _>("status"),
            )
        })
        .collect();

    let ab_rows = sqlx::query("select user_id, id from address_books")
        .fetch_all(&mut conn)
        .await?;
    let abs: Vec<(i64, String)> = ab_rows
        .iter()
        .map(|r| (r.get::<i64, _>("user_id"), r.get::<String, _>("id")))
        .collect();

    db.replace_all_sync(&peers, &users, &abs).await?;

    let cleaned = db.clean_peers_not_in_sync().await?;
    if cleaned > 0 {
        log::warn!("cleaned {} ghost peers not in api list", cleaned);
    }

    log::info!(
        "sync ok: peers={} users={} ab={}",
        peers.len(),
        users.len(),
        abs.len()
    );
    Ok(())
}