import postgres from "postgres";

import {
	TableConfig,
	IProviderOps,
	IProvider,
	providerAdapter
} from "./common.js";

import type { Adapter, InitializeAdapter } from "lucia";

export class ProviderOps implements IProviderOps {
	sql: postgres.Sql;

	constructor(sql: postgres.Sql) {
		this.sql = sql;
	}

	async exec(query: string, arg?: any[]) {
		return this.sql.unsafe(query, arg);
	}

	async get<T>(query: string, arg?: any[]) {
		const res = await this.sql.unsafe(query, arg);
		return (res.at(0) ?? null) as T;
	}

	async getAll<T>(query: string, arg?: any[]) {
		return Array.from(await this.sql.unsafe(query, arg)) as T[];
	}

	processException(e: any) {
		return e as Partial<postgres.PostgresError>;
	}
}

type TXHandler = (fn: ProviderOps) => Promise<any>;

export class Provider extends ProviderOps implements IProvider {
	constructor(sql: postgres.Sql) {
		super(sql);
	}

	async transaction(execute: TXHandler) {
		return await this.sql.begin(async (sql: postgres.Sql) => {
			return await execute(new ProviderOps(sql));
		});
	}
}

export const postgresAdapter = (
	pool: postgres.Sql,
	tables: TableConfig
): InitializeAdapter<Adapter> => {
	const p = new Provider(pool);
	return providerAdapter(p, tables);
};

export type { PgSession } from "./common.js";
export { transformPgSession } from "./common.js";
