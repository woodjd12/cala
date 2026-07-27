use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

#[derive(Builder, Clone, Debug)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct CalaLedgerConfig {
    #[builder(setter(into, strip_option), default)]
    pub(super) pg_con: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub(super) max_connections: Option<u32>,
    #[builder(default)]
    pub(super) exec_migrations: bool,
    #[builder(setter(into, strip_option), default)]
    pub(super) pool: Option<sqlx::PgPool>,
    #[builder(setter(into), default = "Clock::handle().clone()")]
    pub(super) clock: ClockHandle,
    /// Have cala host the streaming balance projector in a job runtime it
    /// owns. EC account sets are maintained only by the projector (see
    /// [`crate::projector`]), so exactly one must run somewhere: with
    /// `true` cala spawns it into its own [`job::Jobs`] runtime at init
    /// (requires the `job` crate's tables); with `false` (default) the
    /// embedder registers
    /// [`BalanceProjectorInit`](crate::projector::BalanceProjectorInit)
    /// in a runtime it already owns.
    #[builder(default)]
    pub(super) ec_balance_projector: bool,
    /// Poller tuning for the job runtime cala hosts; only read when
    /// `ec_balance_projector` is enabled.
    #[builder(default)]
    pub(super) job_poller_config: job::JobPollerConfig,
}

impl CalaLedgerConfig {
    pub fn builder() -> CalaLedgerConfigBuilder {
        CalaLedgerConfigBuilder::default()
    }
}

impl CalaLedgerConfigBuilder {
    fn validate(&self) -> Result<(), String> {
        match (self.pg_con.as_ref(), self.pool.as_ref()) {
            (None, None) | (Some(None), None) | (None, Some(None)) => {
                return Err("One of pg_con or pool must be set".to_string())
            }
            (Some(_), Some(_)) => return Err("Only one of pg_con or pool must be set".to_string()),
            _ => (),
        }
        Ok(())
    }
}
