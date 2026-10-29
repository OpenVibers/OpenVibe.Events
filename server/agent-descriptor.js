'use strict';
// What openvibe.events tells an agent about itself (openvibe-shared/agent-layer): /.well-known/agent.json.
// Every field is something this host really serves: the product home and its update log, the JSON API under
// /api/v1, the public OpenAPI 3.1 document at /openapi.json, llms.txt and the sitemap. The API is called with
// an OpenVibe.Network service token or a developer-app token; the pages need no sign-in.
module.exports = {
    id: 'events',
    name: 'OpenVibe.Events',
    purpose: 'The OpenVibe network\'s durable event bus: publish once and every subscriber gets it signed, retried and replayable, by webhook, pull or realtime stream, with dead-letter inspection and replay.',
    home: 'https://openvibe.events',
    site: 'openvibe.events',
    api: '/api/v1',
    openapi: '/openapi.json',
    policy: {
        auth: ['none', 'Network service token', 'Network developer-app token'],
    },
};
