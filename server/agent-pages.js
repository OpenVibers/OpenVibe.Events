'use strict';
// The Markdown twins of OpenVibe.Events' public pages (openvibe-shared/agent-layer): the product home and the
// update log. A twin is the page's own render — the same renderHome/renderUpdates the routes call, with the same
// public stats — so it cannot drift from the page a browser gets, and neither page has a signed-in variant.
const { renderHome, renderUpdates } = require('./home');

const TWIN_PATH_RE = /^\/(?:updates)?$/;

function createAgentPages({ config, homeStats }) {
    async function renderPublicPage(path) {
        const p = String(path || '/').replace(/\/+$/, '') || '/';
        if (!TWIN_PATH_RE.test(p)) return null;
        if (p === '/updates') return { html: renderUpdates({ siteUrl: config.siteUrl }) };
        const stats = homeStats ? await homeStats.read() : null;
        return { html: renderHome({ siteUrl: config.siteUrl, apiUrl: config.baseUrl, stats }) };
    }

    return { renderPublicPage };
}

module.exports = { createAgentPages, TWIN_PATH_RE };
