'use strict';
/**
 * OpenVibe.Events — tell search engines when the product home and its crawl files change.
 *
 * Nothing on this origin changes on its own: the pages crawlers see (the home and robots.txt,
 * sitemap.xml, llms.txt and llms-full.txt) are fixed by the code, so a release is the only moment
 * they do. server/index.js creates the IndexNow client once at boot (openvibe-shared/indexnow) and
 * hands it here with use(); pingRelease() queues those pages for a single batched POST. With no
 * INDEXNOW_KEY the client is disabled and every call is a no-op: nothing is ever sent.
 */
let client = null;
let base = null;

/** Install the client booted in server/index.js, with the site origin to ping. Disabled → pinging off. */
function use(indexnow, siteUrl) {
    client = indexnow && indexnow.enabled ? indexnow : null;
    base = siteUrl ? String(siteUrl).replace(/\/$/, '') : null;
}

/**
 * Queue the product home and its machine-readable files for one batched ping. Returns true when URLs
 * were queued. Never throws (a failed ping must never fail a boot).
 */
function pingRelease() {
    try {
        if (!client || !base) return false;
        return client.pingSoon([`${base}/`, `${base}/sitemap.xml`, `${base}/llms.txt`, `${base}/llms-full.txt`]) > 0;
    } catch (err) {
        console.warn('[IndexNow] notify failed:', err.message);
        return false;
    }
}

module.exports = { use, pingRelease };
