import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: process.env.SITE_URL ?? 'https://keepsake.pages.dev',
  integrations: [
    starlight({
      title: 'Keepsake',
      description: 'Deterministic relation lifecycles for Rust applications.',
      customCss: ['./src/styles/custom.css'],
      sidebar: [
        {
          label: 'Start Here',
          items: [
            { label: 'Overview', slug: 'start-here/overview' },
            { label: 'Installation', slug: 'start-here/installation' },
            { label: 'Quickstart', slug: 'start-here/quickstart' }
          ]
        },
        {
          label: 'Guides',
          items: [
            { label: 'Tags', slug: 'guides/tags' },
            { label: 'Sanctions', slug: 'guides/sanctions' },
            { label: 'Fulfillment Projections', slug: 'guides/fulfillment-projections' },
            { label: 'Expiry Jobs', slug: 'guides/expiry-jobs' },
            { label: 'Audit Sinks', slug: 'guides/audit-sinks' },
            { label: 'Observability', slug: 'guides/observability' }
          ]
        },
        {
          label: 'Reference',
          items: [
            { label: 'Core Concepts', slug: 'reference/core-concepts' },
            { label: 'Command API', slug: 'reference/command-api' },
            { label: 'SQLx Adapter', slug: 'reference/sqlx-adapter' },
            { label: 'Feature Flags', slug: 'reference/feature-flags' },
            { label: 'Error Model', slug: 'reference/error-model' }
          ]
        },
        {
          label: 'Operations',
          items: [
            { label: 'Migrations', slug: 'operations/migrations' },
            { label: 'Versioning', slug: 'operations/versioning' },
            { label: 'Indexes', slug: 'operations/indexes' },
            { label: 'Cron And Workers', slug: 'operations/cron-workers' },
            { label: 'Query Performance', slug: 'operations/query-performance' }
          ]
        }
      ]
    })
  ]
});
