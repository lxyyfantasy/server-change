use async_trait::async_trait;
use hbb_common::{log, ResultType};
use sqlx::{
    sqlite::SqliteConnectOptions, Acquire, ConnectOptions, Connection, Error as SqlxError,
    SqliteConnection,
};
use std::{ops::DerefMut, str::FromStr};

type Pool = deadpool::managed::Pool<DbPool>;

pub struct DbPool {
    url: String,
}

#[async_trait]
impl deadpool::managed::Manager for DbPool {
    type Type = SqliteConnection;
    type Error = SqlxError;
    async fn create(&self) -> Result<SqliteConnection, SqlxError> {
        let mut opt = SqliteConnectOptions::from_str(&self.url).unwrap();
        opt.log_statements(log::LevelFilter::Debug);
        SqliteConnection::connect_with(&opt).await
    }
    async fn recycle(
        &self,
        obj: &mut SqliteConnection,
    ) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

#[derive(Clone)]
pub struct Database {
    pool: Pool,
}

#[derive(Default)]
pub struct Peer {
    pub guid: Vec<u8>,
    pub id: String,
    pub uuid: Vec<u8>,
    pub pk: Vec<u8>,
    pub user: Option<Vec<u8>>,
    pub info: String,
    pub status: Option<i64>,
}

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        if !std::path::Path::new(url).exists() {
            std::fs::File::create(url).ok();
        }
        let n: usize = std::env::var("MAX_DATABASE_CONNECTIONS")
            .unwrap_or_else(|_| "1".to_owned())
            .parse()
            .unwrap_or(1);
        log::debug!("MAX_DATABASE_CONNECTIONS={}", n);
        let pool = Pool::new(
            DbPool {
                url: url.to_owned(),
            },
            n,
        );
        let _ = pool.get().await?; // test
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> ResultType<()> {
        sqlx::query!(
            "
            create table if not exists peer (
                guid blob primary key not null,
                id varchar(100) not null,
                uuid blob not null,
                pk blob not null,
                created_at datetime not null default(current_timestamp),
                user blob,
                status tinyint,
                note varchar(300),
                info text not null
            ) without rowid;
            create unique index if not exists index_peer_id on peer (id);
            create index if not exists index_peer_user on peer (user);
            create index if not exists index_peer_created_at on peer (created_at);
            create index if not exists index_peer_status on peer (status);
        "
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        sqlx::query(
            "
            create table if not exists sync_peers (
                id text primary key not null,
                lease_expire_at integer not null default 0
            );
            create table if not exists sync_users (
                id integer primary key not null,
                is_admin integer not null default 0,
                status integer not null default 1
            );
            create table if not exists sync_address_books (
                user_id integer not null,
                peer_id text not null,
                primary key (user_id, peer_id)
            );
            ",
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        Ok(sqlx::query_as!(
            Peer,
            "select guid, id, uuid, pk, user, status, info from peer where id = ?",
            id
        )
        .fetch_optional(self.pool.get().await?.deref_mut())
        .await?)
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        sqlx::query!(
            "insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)",
            guid,
            id,
            uuid,
            pk,
            info
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(guid)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        sqlx::query!(
            "update peer set id=?, pk=?, info=? where guid=?",
            id,
            pk,
            info,
            guid
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }

    pub async fn replace_sync_peers(&self, rows: &[(String, i64)]) -> ResultType<()> {
        let mut conn = self.pool.get().await?;
        let mut tx = sqlx::Acquire::begin(&mut *conn).await?;
        sqlx::query("delete from sync_peers")
            .execute(&mut *tx)
            .await?;
        for (id, exp) in rows {
            sqlx::query("insert into sync_peers(id, lease_expire_at) values(?, ?)")
                .bind(id)
                .bind(exp)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn replace_sync_users(&self, rows: &[(i64, i64, i64)]) -> ResultType<()> {
        let mut conn = self.pool.get().await?;
        let mut tx = sqlx::Acquire::begin(&mut *conn).await?;
        sqlx::query("delete from sync_users")
            .execute(&mut *tx)
            .await?;
        for (id, is_admin, status) in rows {
            sqlx::query("insert into sync_users(id, is_admin, status) values(?, ?, ?)")
                .bind(id)
                .bind(is_admin)
                .bind(status)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn replace_sync_address_books(&self, rows: &[(i64, String)]) -> ResultType<()> {
        let mut conn = self.pool.get().await?;
        let mut tx = sqlx::Acquire::begin(&mut *conn).await?;
        sqlx::query("delete from sync_address_books")
            .execute(&mut *tx)
            .await?;
        for (user_id, peer_id) in rows {
            sqlx::query("insert into sync_address_books(user_id, peer_id) values(?, ?)")
                .bind(user_id)
                .bind(peer_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn clean_peers_not_in_sync(&self) -> ResultType<u64> {
        let res = sqlx::query(
            "delete from peer where id not in (select id from sync_peers)"
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(res.rows_affected())
    }

    pub async fn sync_user_is_admin(&self, user_id: u32) -> ResultType<Option<(i64, i64)>> {
        Ok(sqlx::query_as::<_, (i64, i64)>(
            "select is_admin, status from sync_users where id = ?",
        )
        .bind(user_id as i64)
        .fetch_optional(self.pool.get().await?.deref_mut())
        .await?)
    }

    pub async fn sync_user_has_peer(&self, user_id: u32, peer_id: &str) -> ResultType<bool> {
        let row: Option<(i64,)> =
            sqlx::query_as("select 1 from sync_address_books where user_id = ? and peer_id = ?")
                .bind(user_id as i64)
                .bind(peer_id)
                .fetch_optional(self.pool.get().await?.deref_mut())
                .await?;
        Ok(row.is_some())
    }

    pub async fn sync_peer_lease_expire_at(&self, peer_id: &str) -> ResultType<Option<i64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("select lease_expire_at from sync_peers where id = ?")
                .bind(peer_id)
                .fetch_optional(self.pool.get().await?.deref_mut())
                .await?;
        Ok(row.map(|x| x.0))
    }
}

#[cfg(test)]
mod tests {
    use hbb_common::tokio;
    #[test]
    fn test_insert() {
        insert();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn insert() {
        let db = super::Database::new("test.sqlite3").await.unwrap();
        let mut jobs = vec![];
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                let empty_vec = Vec::new();
                cloned
                    .insert_peer(&id, &empty_vec, &empty_vec, "")
                    .await
                    .unwrap();
            });
            jobs.push(a);
        }
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                cloned.get_peer(&id).await.unwrap();
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }
}