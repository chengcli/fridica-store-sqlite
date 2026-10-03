use super::*;

/// A companion file for these tests: a fixed, idempotent text change, no
/// database check, and a fingerprint-checked replacement.
struct TestCompanion;
impl Companion for TestCompanion {
    fn migrate_text(&self, source: &str) -> Result<String> {
        Ok(if source.contains("# migrated\n") {
            source.to_owned()
        } else {
            format!("{source}# migrated\n")
        })
    }
    fn check(&self, _: &str, _: &Path, _: &Path) -> Result<()> {
        Ok(())
    }
    fn replace(&self, path: &Path, expected: &str, text: &str) -> Result<()> {
        if digest(&fs::read(path)?) != expected {
            bail!("configuration changed before replacement");
        }
        atomic_write(path, text.as_bytes())
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    db: PathBuf,
    cfg: PathBuf,
    original: String,
}
impl Fixture {
    fn new() -> Self {
        Self::at_version(5)
    }
    fn at_version(version: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.sqlite3");
        let cfg = dir.path().join("config.toml");
        let c = Connection::open(&db).unwrap();
        for (n, sql) in schema::MIGRATIONS[..version].iter().enumerate() {
            c.execute_batch(sql).unwrap();
            c.execute(
                "INSERT OR REPLACE INTO meta VALUES('schema_version',?)",
                [(n + 1).to_string()],
            )
            .unwrap();
        }
        if version > 0 {
            c.execute("INSERT INTO threads(id,workspace,channel,root_ts,control,pause_reason,created,updated) VALUES('auto','W','C','1','paused','3 turns without progress; review before continuing.',1,1)", []).unwrap();
        }
        let original = "# retain comment\n[owner]\nslack_user='UOWNER'\n[slack]\nworkspace='TTEAM'\nchannels=['CROOM']\n[machines.box]\nhost='box'\n[machines.box.workspaces]\nwork='/work'\n[state]\npath='state.sqlite3'\n[limits]\nmax_wait_replies=5\nmax_no_progress=3\n".to_owned();
        fs::write(&cfg, &original).unwrap();
        Self {
            _dir: dir,
            db,
            cfg,
            original,
        }
    }
    /// A database converted from v5 at some version and used since (#86).
    fn used_at(version: usize) -> Self {
        let f = Self::at_version(version);
        let c = Connection::open(&f.db).unwrap();
        c.execute_batch(
            "INSERT INTO meta VALUES('v6_converted','1'); INSERT INTO meta VALUES('v6_migration_generation','1');",
        )
        .unwrap();
        schema::install_mutation_guards(&c).unwrap();
        c.execute("INSERT INTO threads(id,workspace,channel,root_ts,created,updated) VALUES('used','W','C','9',1,1)", []).unwrap();
        f
    }
    fn interrupt(&self, at: &str) {
        let error = migrate_with_checkpoint(&self.db, &self.cfg, 10., &TestCompanion, |phase| {
            if phase == at {
                bail!("injected interruption at {phase}");
            }
            Ok(())
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("injected interruption"),
            "{error:#}"
        );
    }
}

#[test]
fn every_migration_boundary_recovers_and_retains_one_resume_and_original_backup() {
    for phase in [
        "intent",
        "database_backup",
        "configuration_backup",
        "prepared",
        "schema",
        "conversion",
        "configuration_export",
        "complete",
    ] {
        let f = Fixture::new();
        f.interrupt(phase);
        assert_eq!(check_ready(&f.db).is_ok(), phase == "complete");
        migrate(&f.db, &f.cfg, 20., &TestCompanion).unwrap();
        migrate(&f.db, &f.cfg, 30., &TestCompanion).unwrap();
        check_ready(&f.db).unwrap();
        let c = Connection::open(&f.db).unwrap();
        assert_eq!(schema::version(&c).unwrap(), schema::VERSION);
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM audit WHERE action='migration.resume'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1,
            "{phase}"
        );
        assert_eq!(
            fs::read_to_string(sibling(&f.cfg, &backup_suffix())).unwrap(),
            f.original
        );
        rollback(&f.db, &f.cfg, &TestCompanion).unwrap();
        assert_eq!(schema::version(&c).unwrap(), 5);
        assert_eq!(fs::read_to_string(&f.cfg).unwrap(), f.original);
    }
}

#[test]
fn interrupted_migration_cannot_adopt_later_mutations_as_rollback_baseline() {
    for phase in ["conversion", "configuration_export"] {
        let f = Fixture::new();
        f.interrupt(phase);
        let c = Connection::open(&f.db).unwrap();
        c.execute(
            "INSERT INTO health_events(kind,details_json,created) VALUES('disconnect','{}',11)",
            [],
        )
        .unwrap();
        c.execute("DELETE FROM health_events", []).unwrap();
        assert!(migrate(&f.db, &f.cfg, 20., &TestCompanion)
            .unwrap_err()
            .to_string()
            .contains("durable mutations"));
        assert!(check_ready(&f.db).is_err());
        assert!(rollback(&f.db, &f.cfg, &TestCompanion).is_err());
    }
}

#[test]
fn interrupted_rollback_resumes_its_direction_and_preserves_both_original_files() {
    for phase in [
        "rollback_intent",
        "rollback_database",
        "rollback_configuration",
    ] {
        let f = Fixture::new();
        migrate(&f.db, &f.cfg, 10., &TestCompanion).unwrap();
        let error = rollback_with_checkpoint(&f.db, &f.cfg, &TestCompanion, |at| {
            if at == phase {
                bail!("injected rollback interruption");
            }
            Ok(())
        })
        .unwrap_err();
        assert!(error.to_string().contains("injected rollback interruption"));
        assert!(check_ready(&f.db).is_err());
        assert!(
            migrate(&f.db, &f.cfg, 20., &TestCompanion).is_err(),
            "must not reverse an unfinished rollback"
        );
        rollback(&f.db, &f.cfg, &TestCompanion).unwrap();
        assert_eq!(
            schema::version(&Connection::open(&f.db).unwrap()).unwrap(),
            5
        );
        assert_eq!(fs::read_to_string(&f.cfg).unwrap(), f.original);
    }
}

#[test]
fn every_recognized_schema_upgrades_repeats_and_restores_its_own_backup() {
    for version in 0..schema::VERSION {
        let f = Fixture::at_version(version);
        assert_eq!(
            dry_run(&f.db, &f.cfg, &TestCompanion).unwrap().from,
            version
        );
        migrate(&f.db, &f.cfg, 10., &TestCompanion).unwrap();
        migrate(&f.db, &f.cfg, 20., &TestCompanion).unwrap();
        let c = Connection::open(&f.db).unwrap();
        assert_eq!(schema::version(&c).unwrap(), schema::VERSION);
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM audit WHERE action='migration.resume'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            i64::from(version > 0)
        );
        rollback(&f.db, &f.cfg, &TestCompanion).unwrap();
        assert_eq!(schema::version(&c).unwrap(), version);
        assert_eq!(fs::read_to_string(&f.cfg).unwrap(), f.original);
    }
}

#[test]
fn a_used_database_resumes_or_rolls_back_from_every_interruption() {
    for (version, phase) in (6..schema::VERSION)
        .flat_map(|v| ["schema", "conversion", "configuration_export"].map(|p| (v, p)))
    {
        // Resume: the interrupted upgrade finishes on the next run.
        let f = Fixture::used_at(version);
        f.interrupt(phase);
        migrate(&f.db, &f.cfg, 20., &TestCompanion).unwrap();
        let c = Connection::open(&f.db).unwrap();
        assert_eq!(schema::version(&c).unwrap(), schema::VERSION, "{phase}");
        let journal: serde_json::Value =
            serde_json::from_slice(&fs::read(sibling(&f.db, ".migration.json")).unwrap()).unwrap();
        assert_eq!(journal["phase"], "complete", "{phase}");
        drop(c);
        // Rollback instead: a failed run leaves a `prepared` journal and a
        // half-migrated database, and the backup takes it back as it was.
        let f = Fixture::used_at(version);
        f.interrupt(phase);
        rollback(&f.db, &f.cfg, &TestCompanion).unwrap();
        let c = Connection::open(&f.db).unwrap();
        assert_eq!(schema::version(&c).unwrap(), version, "{phase}");
        assert_eq!(fs::read_to_string(&f.cfg).unwrap(), f.original, "{phase}");
        check_ready(&f.db).unwrap_err();
    }
}
