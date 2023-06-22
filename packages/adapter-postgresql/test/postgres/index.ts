import { testAdapter, Database } from "@lucia-auth/adapter-test";
import { LuciaError } from "lucia";

import { sql } from "./db.js";
import { escapeName, helper } from "../../src/utils.js";
import {
	postgresAdapter,
	transformPgSession,
	Provider
} from "../../src/drivers/postgres.js";
import { ESCAPED_SESSION_TABLE_NAME, TABLE_NAMES } from "../shared.js";

import type { QueryHandler, TableQueryHandler } from "@lucia-auth/adapter-test";
import type { PgSession } from "../../src/drivers/pg.js";

const p = new Provider(sql);

const createTableQueryHandler = (tableName: string): TableQueryHandler => {
	const ESCAPED_TABLE_NAME = escapeName(tableName);
	return {
		get: async () => {
			return await p.getAll(`SELECT * FROM ${ESCAPED_TABLE_NAME}`);
		},
		insert: async (value: any) => {
			const [fields, placeholders, args] = helper(value);
			await p.exec(
				`INSERT INTO ${ESCAPED_TABLE_NAME} ( ${fields} ) VALUES ( ${placeholders} )`,
				args
			);
		},
		clear: async () => {
			await p.exec(`DELETE FROM ${ESCAPED_TABLE_NAME}`);
		}
	};
};

const queryHandler: QueryHandler = {
	user: createTableQueryHandler(TABLE_NAMES.user),
	session: {
		...createTableQueryHandler(TABLE_NAMES.session),
		get: async () => {
			const result = await p.getAll<PgSession>(
				`SELECT * FROM ${ESCAPED_SESSION_TABLE_NAME}`
			);
			return result.map((val) => transformPgSession(val));
		}
	},
	key: createTableQueryHandler(TABLE_NAMES.key)
};

const adapter = postgresAdapter(sql, TABLE_NAMES)(LuciaError);

await testAdapter(adapter, new Database(queryHandler));

process.exit(0);
