import {
	TableConfig,
	IProviderOps,
	IProvider,
	providerAdapter
} from "./common.js";

import { helper, getSetArgs, escapeName } from "../utils.js";

import type { Adapter, InitializeAdapter } from "lucia";

import type {
	QueryResult,
	DatabaseError,
	Pool,
	PoolClient,
	QueryResultRow
} from "pg";

class ProviderOps<T extends Pool | PoolClient> implements IProviderOps {
	pool: T;

	constructor(pool: T) {
		this.pool = pool;
	}

	async exec(query: string, arg?: any[]) {
		return this.pool.query(query, arg);
	}

	async get(query: string, arg?: any[]) {
		return get(this.pool.query(query, arg));
	}

	async getAll(query: string, arg?: any[]) {
		return getAll(this.pool.query(query, arg));
	}

	processException(e: any) {
		return e as Partial<DatabaseError>;
	}
}

type TXHandler = (fn: ProviderOps<PoolClient>) => Promise<any>;

class provider extends ProviderOps<Pool> implements IProvider {
	constructor(pool: Pool) {
		super(pool);
	}

	async transaction(execute: TXHandler) {
		const p = new ProviderOps(await this.pool.connect());
		try {
			await p.exec("BEGIN");
			await execute(p);
			await p.exec("COMMIT");
		} catch (e) {
			p.exec("ROLLBACK");
			throw e;
		}
	}
}

export const pgAdapter = (
	pool: Pool,
	tables: TableConfig
): InitializeAdapter<Adapter> => {
	const p = new provider(pool);
	return providerAdapter(p, tables);
};

export const get = async <_Schema extends QueryResultRow>(
	queryPromise: Promise<QueryResult<_Schema>>
): Promise<_Schema | null> => {
	const { rows } = await queryPromise;
	const result = rows.at(0) ?? null;
	return result;
};

export const getAll = async <_Schema extends QueryResultRow>(
	queryPromise: Promise<QueryResult<_Schema>>
): Promise<_Schema[]> => {
	const { rows } = await queryPromise;
	return rows;
};

export type { PgSession } from "./common.js";
export { transformPgSession } from "./common.js";
