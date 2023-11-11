require('dotenv').config();

import { geoToH3 } from 'h3-js';
import knex from 'knex';

// Set up your database connection
const db = knex({
  client: 'pg',
  connection: {
    host: process.env.DB_HOST,
    port: 5432,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    ssl: {
      rejectUnauthorized: false
    }
  },
  pool: { min: 2, max: 30 },
});

export async function updateRows(limit: number) {
  const rows: any[] = await db('kontur_population_us')
    .whereNull('hex_res8')
    .select(db.raw('ogc_fid, st_x(center) as lng, st_y(center) as lat, population'))
    .limit(limit);

  for (const row of rows) {
    const { ogc_fid, lat, lng } = row;
    const hex_res8 = geoToH3(lat, lng, 8).substring(0, 10);
    await db('kontur_population_us').where({ ogc_fid }).update({ hex_res8 });
  }

  return rows.length;
}

export async function run() {
  const limit = 1000;
  let updated = 0;

  while ((await updateRows(limit)) > 0) {
    updated += limit;
    console.log(`${updated} rows updated`);
  }

  process.exit(1);
}

run();
