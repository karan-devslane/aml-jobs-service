import { Op, Sequelize } from 'sequelize';
import { AppDataSource } from '../config';
import { QuestionSet } from '../models/questionSet';
import logger from '../utils/logger';

export const createQuestionSet = async (insertData: Array<Record<string, any>>): Promise<any> => {
  const transact = await AppDataSource.transaction();
  try {
    await QuestionSet.bulkCreate(insertData, { transaction: transact });
    await transact.commit();
    return { error: false, message: 'success' };
  } catch (error: any) {
    await transact.rollback();
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to create a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const getQuestionSets = async (): Promise<any> => {
  try {
    const questionSets = await QuestionSet.findAll({
      attributes: ['id', 'identifier', 'question_set_id', [Sequelize.literal(`taxonomy->'l1_skill'->'name'->'en'`), 'l1_skill']],
      raw: true,
    });
    return questionSets;
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get records' : '';
    return { error: true, message: errorMsg };
  }
};

export const deleteQuestionSets = async (whereClause: any): Promise<any> => {
  try {
    await QuestionSet.destroy({
      where: {
        identifier: {
          [Op.in]: whereClause,
        },
      },
    });
    return { error: false };
  } catch (error) {
    logger.error(error);
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to delete records' : '';
    return { error: true, message: errorMsg };
  }
};

export const findExistingQuestionSetXIDs = async (xids: string[]): Promise<any> => {
  return QuestionSet.findAll({
    where: {
      x_id: xids,
    },
    raw: true,
    attributes: ['x_id'],
  });
};
