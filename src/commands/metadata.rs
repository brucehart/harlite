//! Privacy commands cannot safely infer the semantics of arbitrary HAR extensions.
//! Remove unscanned copies instead of publishing them alongside sanitized fields.
use crate::error::Result;
use crate::har::{Extensions, Har};
use rusqlite::Connection;

fn clear_extensions(values: &mut Extensions) -> usize {
    let count = usize::from(!values.is_empty());
    values.clear();
    count
}

pub(crate) fn discard_har_metadata(har: &mut Har, dry_run: bool) {
    let mut count = clear_extensions(&mut har.log.extensions);
    for page in har.log.pages.iter_mut().flatten() {
        count += clear_extensions(&mut page.extensions);
        if let Some(timings) = &mut page.page_timings {
            count += clear_extensions(&mut timings.extensions);
        }
    }
    for entry in &mut har.log.entries {
        count += clear_extensions(&mut entry.extensions);
        count += clear_extensions(&mut entry.request.extensions);
        count += clear_extensions(&mut entry.response.extensions);
        count += clear_extensions(&mut entry.response.content.extensions);
        if let Some(timings) = &mut entry.timings {
            count += clear_extensions(&mut timings.extensions);
        }
        if let Some(post) = &mut entry.request.post_data {
            count += clear_extensions(&mut post.extensions);
        }
        count += usize::from(entry.cache.take().is_some());
        count += usize::from(
            entry
                .response
                .redirect_url
                .take()
                .is_some_and(|url| !url.is_empty()),
        );
    }
    report(count, dry_run);
}

pub(crate) fn discard_database_metadata(conn: &Connection, write: bool) -> Result<()> {
    let mut count = 0usize;
    for (table, columns) in [
        ("imports", &["log_extensions"][..]),
        ("pages", &["page_extensions", "page_timings_extensions"][..]),
        (
            "entries",
            &[
                "entry_extensions",
                "request_extensions",
                "response_extensions",
                "content_extensions",
                "timings_extensions",
                "post_data_extensions",
                "initiator_url",
                "redirect_url",
            ][..],
        ),
    ] {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let actual = stmt
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for column in columns
            .iter()
            .filter(|column| actual.iter().any(|name| name == **column))
        {
            count += conn.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"),
                [],
                |r| r.get::<_, usize>(0),
            )?;
            if write {
                conn.execute(
                    &format!("UPDATE {table} SET {column}=NULL WHERE {column} IS NOT NULL"),
                    [],
                )?;
            }
        }
    }
    report(count, !write);
    Ok(())
}

fn report(count: usize, dry_run: bool) {
    if count > 0 {
        let action = if dry_run {
            "Would discard"
        } else {
            "Discarded"
        };
        eprintln!("{action} {count} unscanned metadata containers/URL copies; privacy output does not preserve HAR extensions or cache metadata.");
    }
}
