import 'reflect-metadata';
import { Sequelize } from 'sequelize-typescript';
import appConfiguration from './config';

const {
  DB: { port, name, password, host, user },
} = appConfiguration;

const AppDataSource = new Sequelize({
  dialect: 'postgres',
  host: host,
  port: port,
  username: user,
  password: password,
  database: name,
  models: [__dirname + '/models/*.ts'],
  logging: false,
});

export const query = async (query: string) => {
  const [results, metadata] = await AppDataSource.query(query);
  return {
    results,
    metadata,
  };
};

export default AppDataSource;
