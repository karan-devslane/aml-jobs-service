import cors from 'cors';
import express, { Application, NextFunction, Request, Response } from 'express';
import { NOT_FOUND } from 'http-status';
import { appConfiguration, AppDataSource } from './config';
import logger from './utils/logger';
import bodyParser from 'body-parser';
import { router } from './routes/router';

const { envPort } = appConfiguration;

const app: Application = express();

const initializeServer = (): void => {
  // Middleware setup
  app.use(bodyParser.json({ limit: '5mb' }));
  app.use(bodyParser.urlencoded({ limit: '5mb', extended: true, parameterLimit: 50000 }));
  app.use(cors());

  // Router setup
  app.use('/api/v1', router);

  // 404 handler for unknown API requests
  app.use((req: Request, res: Response, next: NextFunction) => {
    next({ statusCode: NOT_FOUND, message: 'Not found' });
  });

  // Database connection
  AppDataSource.authenticate()
    .then(() => {
      logger.info('Database connected successfully');

      // Start the server
      const server = app.listen(envPort, () => {
        logger.info(`Listening on port ${envPort}`);
      });

      // Graceful shutdown
      const exitHandler = (): void => {
        server.close(() => {
          logger.info('Server closed');
          process.exit(0);
        });
      };

      const unexpectedErrorHandler = (error: Error): void => {
        logger.error('Unexpected error', error);
        exitHandler();
      };

      process.on('uncaughtException', unexpectedErrorHandler);
      process.on('unhandledRejection', unexpectedErrorHandler);
    })
    .catch((err) => {
      logger.error(`Error in database connection: ${err.message || err}`);
      process.exit(1); // Exit if database connection fails
    });
};

// Start the server
initializeServer();

// Export for testing
export default app;
