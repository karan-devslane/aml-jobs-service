import { ContentStage } from '../models/contentStage';
import logger from '../utils/logger';

export const createContentStage = async (insertData: Record<string, unknown>[]): Promise<any> => {
  try {
    const stagingData = await ContentStage.bulkCreate(insertData);
    const [dataValues] = stagingData;
    return { dataValues };
  } catch (error) {
    logger.error(error);
  }
};

export const contentStageMetaData = async (whereClause: any): Promise<any> => {
  try {
    const Contents = await ContentStage.findAll({ where: whereClause });
    const contents = Contents.map((c) => c.dataValues);
    return contents;
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const updateContentStage = async (whereClause: any, updateObj: any): Promise<any> => {
  try {
    const updateContent = await ContentStage.update(updateObj, { where: whereClause });
    return { error: false, updateContent };
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to update a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const getAllStageContent = async (): Promise<any> => {
  const Contents = await ContentStage.findAll({});
  const contents = Contents.map((c) => c.dataValues);
  return contents;
};
