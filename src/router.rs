use clap::CommandFactory;

use crate::cli::{Cli, Commands};
use crate::commands::{
    run_analyze, run_cdp, run_diff, run_export, run_export_data, run_fts_rebuild, run_import,
    run_imports, run_info, run_merge, run_openapi, run_otel, run_pii, run_prune, run_query,
    run_redact, run_repl, run_replay, run_schema, run_search, run_serve, run_stats, run_watch,
    run_waterfall, AnalyzeOptions, CdpOptions, DiffOptions, EntryFilterOptions, ExportDataOptions,
    ExportOptions, ImportOptions, InfoOptions, MergeOptions, OpenApiOptions, OtelExportOptions,
    PiiOptions, QueryOptions, RedactOptions, ReplOptions, ReplayOptions, ServeOptions,
    StatsOptions, WatchOptions, WaterfallFormat, WaterfallGroupBy, WaterfallOptions,
};
use crate::config::{load_config, render_config, ResolvedConfig};
use crate::error::Result;
use crate::plugins::resolve_plugins;
use crate::size;

pub fn run(cli: Cli) -> Result<()> {
    let config = load_config()?;
    let resolved = ResolvedConfig::from_config(&config);

    match cli.command {
        Commands::Import {
            files,
            output,
            bodies,
            max_body_size,
            text_only,
            stats,
            incremental,
            resume,
            jobs,
            async_read,
            decompress_bodies,
            keep_compressed,
            extract_bodies,
            extract_bodies_kind,
            extract_bodies_shard_depth,
            host,
            method,
            status,
            url_regex,
            from,
            to,
            plugin,
            disable_plugin,
        } => {
            let defaults = &resolved.import;
            let plugins = resolve_plugins(
                &config.plugins,
                &plugin.unwrap_or_default(),
                &disable_plugin.unwrap_or_default(),
            )?;
            let max_body_size = size::parse_size_bytes_usize(
                &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
            )?;
            let options = ImportOptions {
                output: output.or_else(|| defaults.output.clone()),
                store_bodies: bodies.unwrap_or(defaults.bodies),
                max_body_size,
                text_only: text_only.unwrap_or(defaults.text_only),
                show_stats: stats.unwrap_or(defaults.stats),
                incremental: incremental.unwrap_or(defaults.incremental),
                resume: resume.unwrap_or(defaults.resume),
                jobs: jobs.unwrap_or(defaults.jobs),
                async_read: async_read.unwrap_or(defaults.async_read),
                decompress_bodies: decompress_bodies.unwrap_or(defaults.decompress_bodies),
                keep_compressed: keep_compressed.unwrap_or(defaults.keep_compressed),
                extract_bodies_dir: extract_bodies.or_else(|| defaults.extract_bodies.clone()),
                extract_bodies_kind: extract_bodies_kind.unwrap_or(defaults.extract_bodies_kind),
                extract_bodies_shard_depth: extract_bodies_shard_depth
                    .unwrap_or(defaults.extract_bodies_shard_depth),
                host: host.unwrap_or_else(|| defaults.host.clone()),
                method: method.unwrap_or_else(|| defaults.method.clone()),
                status: status.unwrap_or_else(|| defaults.status.clone()),
                url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                from: from.or_else(|| defaults.from.clone()),
                to: to.or_else(|| defaults.to.clone()),
                plugins,
            };
            run_import(&files, &options).map(|_| ())
        }

        Commands::Cdp {
            host,
            port,
            target,
            har,
            output,
            bodies,
            max_body_size,
            text_only,
            duration,
        } => {
            let defaults = &resolved.cdp;
            let max_body_size = size::parse_size_bytes_usize(
                &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
            )?;
            let options = CdpOptions {
                host: host.unwrap_or_else(|| defaults.host.clone()),
                port: port.unwrap_or(defaults.port),
                target: target.or_else(|| defaults.target.clone()),
                output_har: har.or_else(|| defaults.har.clone()),
                output_db: output.or_else(|| defaults.output.clone()),
                store_bodies: bodies.unwrap_or(defaults.bodies),
                max_body_size,
                text_only: text_only.unwrap_or(defaults.text_only),
                duration_secs: duration.or(defaults.duration),
            };
            run_cdp(&options)
        }

        Commands::Watch {
            directory,
            output,
            recursive,
            debounce_ms,
            stable_ms,
            import_existing,
            post_info,
            post_stats,
            post_stats_json,
            bodies,
            max_body_size,
            text_only,
            stats,
            incremental,
            resume,
            async_read,
            decompress_bodies,
            keep_compressed,
            extract_bodies,
            extract_bodies_kind,
            extract_bodies_shard_depth,
            host,
            method,
            status,
            url_regex,
            from,
            to,
            plugin,
            disable_plugin,
        } => {
            let defaults = &resolved.import;
            let output_override = output.clone();
            let plugins = resolve_plugins(
                &config.plugins,
                &plugin.unwrap_or_default(),
                &disable_plugin.unwrap_or_default(),
            )?;
            let max_body_size = size::parse_size_bytes_usize(
                &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
            )?;
            let import_options = ImportOptions {
                output: output_override.clone().or_else(|| defaults.output.clone()),
                store_bodies: bodies.unwrap_or(defaults.bodies),
                max_body_size,
                text_only: text_only.unwrap_or(defaults.text_only),
                show_stats: stats.unwrap_or(defaults.stats),
                incremental: incremental.unwrap_or(defaults.incremental),
                resume: resume.unwrap_or(defaults.resume),
                jobs: 1,
                async_read: async_read.unwrap_or(defaults.async_read),
                decompress_bodies: decompress_bodies.unwrap_or(defaults.decompress_bodies),
                keep_compressed: keep_compressed.unwrap_or(defaults.keep_compressed),
                extract_bodies_dir: extract_bodies.or_else(|| defaults.extract_bodies.clone()),
                extract_bodies_kind: extract_bodies_kind.unwrap_or(defaults.extract_bodies_kind),
                extract_bodies_shard_depth: extract_bodies_shard_depth
                    .unwrap_or(defaults.extract_bodies_shard_depth),
                host: host.unwrap_or_else(|| defaults.host.clone()),
                method: method.unwrap_or_else(|| defaults.method.clone()),
                status: status.unwrap_or_else(|| defaults.status.clone()),
                url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                from: from.or_else(|| defaults.from.clone()),
                to: to.or_else(|| defaults.to.clone()),
                plugins,
            };

            let watch_options = WatchOptions {
                output: output_override.or_else(|| defaults.output.clone()),
                recursive: recursive.unwrap_or(true),
                debounce_ms: debounce_ms.unwrap_or(750),
                stable_ms: stable_ms.unwrap_or(1000),
                import_existing: import_existing.unwrap_or(false),
                post_info: post_info.unwrap_or(false),
                post_stats: post_stats.unwrap_or(false) || post_stats_json.unwrap_or(false),
                post_stats_json: post_stats_json.unwrap_or(false),
                import_options,
            };

            run_watch(directory, &watch_options)
        }

        Commands::Config => {
            let rendered = render_config(&resolved)?;
            println!("{rendered}");
            Ok(())
        }

        Commands::Schema { database } => run_schema(database),

        Commands::Info {
            database,
            cert_expiring_days,
        } => {
            let options = InfoOptions { cert_expiring_days };
            run_info(database, &options)
        }

        Commands::Imports { database } => run_imports(database),

        Commands::Prune {
            database,
            import_id,
        } => run_prune(database, import_id),

        Commands::Stats {
            database,
            json,
            cert_expiring_days,
        } => {
            let defaults = &resolved.stats;
            let options = StatsOptions {
                json: json.unwrap_or(defaults.json),
                cert_expiring_days: cert_expiring_days.or(defaults.cert_expiring_days),
            };
            run_stats(database, &options)
        }

        Commands::Analyze {
            database,
            json,
            host,
            method,
            status,
            from,
            to,
            slow_total_ms,
            slow_ttfb_ms,
            top,
        } => {
            let options = AnalyzeOptions {
                json: json.unwrap_or(false),
                host: host.unwrap_or_default(),
                method: method.unwrap_or_default(),
                status: status.unwrap_or_default(),
                from,
                to,
                slow_total_ms: slow_total_ms.unwrap_or(1000.0),
                slow_ttfb_ms: slow_ttfb_ms.unwrap_or(500.0),
                top: top.unwrap_or(10),
            };
            run_analyze(database, &options)
        }

        Commands::Export {
            database,
            output,
            bodies,
            bodies_raw,
            allow_external_paths,
            external_path_root,
            compact,
            url,
            url_contains,
            url_regex,
            host,
            method,
            status,
            mime,
            ext,
            source,
            source_contains,
            from,
            to,
            min_request_size,
            max_request_size,
            min_response_size,
            max_response_size,
            plugin,
            disable_plugin,
        } => {
            let defaults = &resolved.export;
            let plugins = resolve_plugins(
                &config.plugins,
                &plugin.unwrap_or_default(),
                &disable_plugin.unwrap_or_default(),
            )?;
            let compact = compact.unwrap_or(defaults.compact);
            let bodies = bodies.unwrap_or(defaults.bodies);
            let bodies_raw = bodies_raw.unwrap_or(defaults.bodies_raw);
            let options = ExportOptions {
                output: output.or_else(|| defaults.output.clone()),
                pretty: !compact,
                include_bodies: bodies || bodies_raw,
                include_raw_response_bodies: bodies_raw,
                allow_external_paths: allow_external_paths.unwrap_or(defaults.allow_external_paths),
                external_path_root: external_path_root
                    .or_else(|| defaults.external_path_root.clone()),
                url: url.unwrap_or_else(|| defaults.url.clone()),
                url_contains: url_contains.unwrap_or_else(|| defaults.url_contains.clone()),
                url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                host: host.unwrap_or_else(|| defaults.host.clone()),
                method: method.unwrap_or_else(|| defaults.method.clone()),
                status: status.unwrap_or_else(|| defaults.status.clone()),
                mime_contains: mime.unwrap_or_else(|| defaults.mime.clone()),
                ext: ext.unwrap_or_else(|| defaults.ext.clone()),
                source: source.unwrap_or_else(|| defaults.source.clone()),
                source_contains: source_contains
                    .unwrap_or_else(|| defaults.source_contains.clone()),
                from: from.or_else(|| defaults.from.clone()),
                to: to.or_else(|| defaults.to.clone()),
                min_request_size: min_request_size.or_else(|| defaults.min_request_size.clone()),
                max_request_size: max_request_size.or_else(|| defaults.max_request_size.clone()),
                min_response_size: min_response_size.or_else(|| defaults.min_response_size.clone()),
                max_response_size: max_response_size.or_else(|| defaults.max_response_size.clone()),
                plugins,
            };
            run_export(database, &options)
        }

        Commands::ExportData {
            database,
            output,
            format,
            url,
            url_contains,
            url_regex,
            host,
            method,
            status,
            mime,
            ext,
            source,
            source_contains,
            from,
            to,
            min_request_size,
            max_request_size,
            min_response_size,
            max_response_size,
        } => {
            let filters = EntryFilterOptions {
                url: url.unwrap_or_default(),
                url_contains: url_contains.unwrap_or_default(),
                url_regex: url_regex.unwrap_or_default(),
                host: host.unwrap_or_default(),
                method: method.unwrap_or_default(),
                status: status.unwrap_or_default(),
                mime_contains: mime.unwrap_or_default(),
                ext: ext.unwrap_or_default(),
                source: source.unwrap_or_default(),
                source_contains: source_contains.unwrap_or_default(),
                from,
                to,
                min_request_size,
                max_request_size,
                min_response_size,
                max_response_size,
            };
            let options = ExportDataOptions {
                output,
                format,
                filters,
            };
            run_export_data(database, &options)
        }

        Commands::Otel {
            database,
            format,
            output,
            endpoint,
            service_name,
            resource_attr,
            no_phases,
            sample_rate,
            max_spans,
            url,
            url_contains,
            url_regex,
            host,
            method,
            status,
            mime,
            ext,
            source,
            source_contains,
            from,
            to,
            min_request_size,
            max_request_size,
            min_response_size,
            max_response_size,
        } => {
            let filters = EntryFilterOptions {
                url: url.unwrap_or_default(),
                url_contains: url_contains.unwrap_or_default(),
                url_regex: url_regex.unwrap_or_default(),
                host: host.unwrap_or_default(),
                method: method.unwrap_or_default(),
                status: status.unwrap_or_default(),
                mime_contains: mime.unwrap_or_default(),
                ext: ext.unwrap_or_default(),
                source: source.unwrap_or_default(),
                source_contains: source_contains.unwrap_or_default(),
                from,
                to,
                min_request_size,
                max_request_size,
                min_response_size,
                max_response_size,
            };
            let options = OtelExportOptions {
                format,
                output,
                endpoint,
                service_name,
                resource_attr: resource_attr.unwrap_or_default(),
                include_phases: !no_phases,
                sample_rate,
                max_spans,
                filters,
            };
            run_otel(database, &options)
        }

        Commands::OpenApi {
            database,
            output,
            title,
            version,
            sample_bodies,
            sample_body_max_size,
            allow_external_paths,
            external_path_root,
            url,
            url_contains,
            url_regex,
            host,
            method,
            status,
            mime,
            ext,
            source,
            source_contains,
            from,
            to,
            min_request_size,
            max_request_size,
            min_response_size,
            max_response_size,
        } => {
            let filters = EntryFilterOptions {
                url: url.unwrap_or_default(),
                url_contains: url_contains.unwrap_or_default(),
                url_regex: url_regex.unwrap_or_default(),
                host: host.unwrap_or_default(),
                method: method.unwrap_or_default(),
                status: status.unwrap_or_default(),
                mime_contains: mime.unwrap_or_default(),
                ext: ext.unwrap_or_default(),
                source: source.unwrap_or_default(),
                source_contains: source_contains.unwrap_or_default(),
                from,
                to,
                min_request_size,
                max_request_size,
                min_response_size,
                max_response_size,
            };
            let options = OpenApiOptions {
                output,
                title,
                version,
                sample_bodies,
                sample_body_max_size,
                allow_external_paths: allow_external_paths.unwrap_or(false),
                external_path_root,
                filters,
            };
            run_openapi(database, &options)
        }

        Commands::Waterfall {
            database,
            output,
            format,
            group_by,
            host,
            page,
            from,
            to,
            width,
        } => {
            let options = WaterfallOptions {
                output,
                format: format.unwrap_or(WaterfallFormat::Text),
                group_by: group_by.unwrap_or(WaterfallGroupBy::Page),
                host: host.unwrap_or_default(),
                page: page.unwrap_or_default(),
                from,
                to,
                width,
            };
            run_waterfall(database, &options)
        }

        Commands::Redact {
            output,
            force,
            dry_run,
            no_defaults,
            header,
            cookie,
            query_param,
            body_regex,
            match_mode,
            token,
            database,
        } => {
            let defaults = &resolved.redact;
            let options = RedactOptions {
                output: output.or_else(|| defaults.output.clone()),
                force: force.unwrap_or(defaults.force),
                dry_run: dry_run.unwrap_or(defaults.dry_run),
                no_defaults: no_defaults.unwrap_or(defaults.no_defaults),
                headers: header.unwrap_or_else(|| defaults.header.clone()),
                cookies: cookie.unwrap_or_else(|| defaults.cookie.clone()),
                query_params: query_param.unwrap_or_else(|| defaults.query_param.clone()),
                body_regexes: body_regex.unwrap_or_else(|| defaults.body_regex.clone()),
                match_mode: match_mode.unwrap_or(defaults.match_mode),
                token: token.unwrap_or_else(|| defaults.token.clone()),
            };
            run_redact(database, &options)
        }

        Commands::Pii {
            format,
            redact,
            output,
            force,
            dry_run,
            no_defaults,
            no_email,
            no_phone,
            no_ssn,
            no_credit_card,
            email_regex,
            phone_regex,
            ssn_regex,
            credit_card_regex,
            token,
            database,
        } => {
            let defaults = &resolved.pii;
            let options = PiiOptions {
                format: format.unwrap_or(defaults.format),
                redact: redact.unwrap_or(defaults.redact),
                output: output.or_else(|| defaults.output.clone()),
                force: force.unwrap_or(defaults.force),
                dry_run: dry_run.unwrap_or(defaults.dry_run),
                no_defaults: no_defaults.unwrap_or(defaults.no_defaults),
                no_email: no_email.unwrap_or(defaults.no_email),
                no_phone: no_phone.unwrap_or(defaults.no_phone),
                no_ssn: no_ssn.unwrap_or(defaults.no_ssn),
                no_credit_card: no_credit_card.unwrap_or(defaults.no_credit_card),
                email_regexes: email_regex.unwrap_or_else(|| defaults.email_regex.clone()),
                phone_regexes: phone_regex.unwrap_or_else(|| defaults.phone_regex.clone()),
                ssn_regexes: ssn_regex.unwrap_or_else(|| defaults.ssn_regex.clone()),
                credit_card_regexes: credit_card_regex
                    .unwrap_or_else(|| defaults.credit_card_regex.clone()),
                token: token.unwrap_or_else(|| defaults.token.clone()),
            };
            run_pii(database, &options)
        }

        Commands::Diff {
            left,
            right,
            format,
            host,
            method,
            status,
            url_regex,
        } => {
            let defaults = &resolved.diff;
            let options = DiffOptions {
                format: format.unwrap_or(defaults.format),
                host: host.unwrap_or_else(|| defaults.host.clone()),
                method: method.unwrap_or_else(|| defaults.method.clone()),
                status: status.unwrap_or_else(|| defaults.status.clone()),
                url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
            };
            run_diff(left, right, &options)
        }

        Commands::Replay {
            input,
            format,
            concurrency,
            rate_limit,
            timeout,
            allow_unsafe,
            allow_external_paths,
            external_path_root,
            url,
            url_contains,
            url_regex,
            host,
            method,
            status,
            override_host,
            override_header,
        } => {
            let defaults = &resolved.replay;
            let options = ReplayOptions {
                format: format.unwrap_or(defaults.format),
                concurrency: concurrency.unwrap_or(defaults.concurrency),
                rate_limit: rate_limit.or(defaults.rate_limit),
                timeout_secs: timeout.or(defaults.timeout_secs),
                allow_unsafe: allow_unsafe.unwrap_or(defaults.allow_unsafe),
                allow_external_paths: allow_external_paths.unwrap_or(defaults.allow_external_paths),
                external_path_root: external_path_root
                    .or_else(|| defaults.external_path_root.clone()),
                url: url.unwrap_or_else(|| defaults.url.clone()),
                url_contains: url_contains.unwrap_or_else(|| defaults.url_contains.clone()),
                url_regex: url_regex.unwrap_or_else(|| defaults.url_regex.clone()),
                host: host.unwrap_or_else(|| defaults.host.clone()),
                method: method.unwrap_or_else(|| defaults.method.clone()),
                status: status.unwrap_or_else(|| defaults.status.clone()),
                override_host: override_host.unwrap_or_else(|| defaults.override_host.clone()),
                override_header: override_header
                    .unwrap_or_else(|| defaults.override_header.clone()),
            };
            run_replay(input, &options)
        }

        Commands::Serve {
            input,
            bind,
            port,
            match_mode,
            allow_external_paths,
            external_path_root,
            tls_cert,
            tls_key,
        } => {
            let options = ServeOptions {
                bind,
                port,
                match_mode,
                allow_external_paths: allow_external_paths.unwrap_or(false),
                external_path_root,
                tls_cert,
                tls_key,
            };
            run_serve(input, &options)
        }

        Commands::Merge {
            databases,
            output,
            dry_run,
            dedup,
        } => {
            let defaults = &resolved.merge;
            let options = MergeOptions {
                output: output.or_else(|| defaults.output.clone()),
                dry_run: dry_run.unwrap_or(defaults.dry_run),
                dedup: dedup.unwrap_or(defaults.dedup),
            };
            run_merge(databases, &options)
        }

        Commands::Query {
            sql,
            database,
            format,
            limit,
            offset,
            quiet,
            no_quiet,
        } => {
            let defaults = &resolved.query;
            let quiet = if no_quiet { Some(false) } else { quiet };
            let options = QueryOptions {
                format: format.unwrap_or(defaults.format),
                limit: limit.or(defaults.limit),
                offset: offset.or(defaults.offset),
                quiet: quiet.unwrap_or(defaults.quiet),
            };
            run_query(sql, database, &options)
        }

        Commands::Search {
            query,
            database,
            format,
            limit,
            offset,
            quiet,
            no_quiet,
        } => {
            let defaults = &resolved.search;
            let quiet = if no_quiet { Some(false) } else { quiet };
            let options = QueryOptions {
                format: format.unwrap_or(defaults.format),
                limit: limit.or(defaults.limit),
                offset: offset.or(defaults.offset),
                quiet: quiet.unwrap_or(defaults.quiet),
            };
            run_search(query, database, &options)
        }

        Commands::Repl { database, format } => {
            let defaults = &resolved.repl;
            let options = ReplOptions {
                format: format.unwrap_or(defaults.format),
            };
            run_repl(database, &options)
        }

        Commands::FtsRebuild {
            database,
            tokenizer,
            max_body_size,
            allow_external_paths,
            external_path_root,
        } => {
            let defaults = &resolved.fts_rebuild;
            let max_body_size = size::parse_size_bytes_usize(
                &max_body_size.unwrap_or_else(|| defaults.max_body_size.clone()),
            )?;
            run_fts_rebuild(
                database,
                tokenizer.unwrap_or(defaults.tokenizer),
                max_body_size,
                allow_external_paths.unwrap_or(defaults.allow_external_paths),
                external_path_root.or_else(|| defaults.external_path_root.clone()),
            )
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(shell, &mut cmd, "harlite", &mut std::io::stdout());
            Ok(())
        }
    }
}
