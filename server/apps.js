'use strict';
/**
 * Developer apps (roadmap Wave 20, ADR-014; capabilities events.app.publish|read|subscribe).
 *
 * An app token from OpenVibe.Network has sub `app:app_<ULID>`, actor_type `app`, `project_id`
 * `prj_<ULID>`, `env` sandbox|production and ns [project_id]. Here it becomes an app principal:
 *
 *   project_key  'p' + the project's ULID in lowercase     prj_01JAB…  -> p01jab…
 *   source       'app-' + the app's ULID in lowercase      app_01JAB…  -> app-01jab…
 *   prefix       'app.<project_key>.'                      the only event types it may publish
 *
 * Both derived names fit the existing events.event-envelope@1 patterns (a type segment is
 * [a-z0-9_]+, a source is [a-z][a-z0-9-]{1,39}), so the envelope contract is unchanged.
 *
 * Scope (what an app sees, pulls and gets delivered):
 *   - its own project's events, in the SAME environment as the token (sandbox never meets production)
 *   - first-party platform events with visibility `public` (always production)
 * First-party readers (services with events.event.read) never see sandbox events, and see app
 * events only through a pattern that starts with the literal segment `app`.
 */
const topics = require('./topics');

const ULID = '[0-9A-HJKMNP-TV-Z]{26}';
const APP_SUB_RE = new RegExp(`^app:app_(${ULID})$`);
const PROJECT_RE = new RegExp(`^prj_(${ULID})$`);
const ENVS = ['sandbox', 'production'];

/** 'prj_01JAB…' -> 'p01jab…' (null for anything that is not a project id). */
function projectKey(projectId) {
    const m = PROJECT_RE.exec(String(projectId || ''));
    return m ? `p${m[1].toLowerCase()}` : null;
}

/** 'app:app_01JAB…' (or 'app_01JAB…') -> 'app-01jab…'. */
function appSource(sub) {
    const s = String(sub || '');
    const m = APP_SUB_RE.exec(s.startsWith('app:') ? s : `app:${s}`);
    return m ? `app-${m[1].toLowerCase()}` : null;
}

/**
 * The app principal for verified token claims, or { error } when the claims are not a usable app
 * token (no project, unknown env, project outside ns).
 */
function appPrincipal(claims) {
    const m = APP_SUB_RE.exec(String(claims && claims.sub));
    if (!m) return { error: 'not an app token' };
    if (claims.actor_type && claims.actor_type !== 'app') return { error: 'actor_type must be app' };
    const key = projectKey(claims.project_id);
    if (!key) return { error: 'app token without a project_id' };
    const env = claims.env === undefined ? 'production' : claims.env;
    if (!ENVS.includes(env)) return { error: `unknown env ${claims.env}` };
    if (Array.isArray(claims.ns) && claims.ns.length && !claims.ns.includes(claims.project_id)) {
        return { error: 'project_id is not in the token namespaces' };
    }
    return {
        kind: 'app',
        sub: claims.sub,
        appId: `app_${m[1]}`,
        projectId: claims.project_id,
        projectKey: key,
        source: `app-${m[1].toLowerCase()}`,
        prefix: `app.${key}.`,
        env,
        onBehalfOf: typeof claims.on_behalf_of === 'string' ? claims.on_behalf_of : null,
    };
}

/**
 * May an app use this topic pattern (pull, checkpoint, subscription)? The first segment must be
 * literal; an `app` pattern must name the app's own project_key as its second segment.
 * Returns null when allowed, or the reason.
 */
function patternScopeError(pattern, app) {
    if (!topics.isValidPattern(pattern)) return 'topic patterns are dot-separated segments of [a-z0-9_] or *';
    const [first, second] = pattern.split('.');
    if (first === '*') return 'an app topic pattern must start with a literal segment (e.g. app.<project_key>.* or live.*)';
    if (first === 'app' && second !== app.projectKey) return `app.* patterns must name your project: app.${app.projectKey}.*`;
    return null;
}

/** Row visibility for an app scope ({ projectId, env }): own project + same env, or public first-party. */
function visibleToApp(row, scope) {
    if (row.project_id) return row.project_id === scope.projectId && (row.env || 'production') === scope.env;
    return row.visibility === 'public' && (row.env || 'production') === 'production';
}

/** Does a first-party (service) pattern select this row? Never sandbox; app events only via `app.` patterns. */
function serviceMatches(pattern, row) {
    if ((row.env || 'production') !== 'production') return false;
    if (row.project_id && !pattern.startsWith('app.')) return false;
    return topics.matches(pattern, row.event_type);
}

/** Should `sub` (a subscriptions row) receive `row` (an events row)? */
function deliverable(sub, row) {
    if (sub.project_id) {
        return topics.matches(sub.topic_pattern, row.event_type) && visibleToApp(row, { projectId: sub.project_id, env: sub.env || 'production' });
    }
    return serviceMatches(sub.topic_pattern, row);
}

module.exports = { projectKey, appSource, appPrincipal, patternScopeError, visibleToApp, serviceMatches, deliverable, ENVS, APP_SUB_RE };
