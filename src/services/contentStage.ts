import { ContentStage } from '../models/contentStage';
import { Optional } from 'sequelize';
import logger from '../utils/logger';

export const createContentStage = async (req: Optional<any, any>[]): Promise<any> => {
  try {
    const stagingData = await ContentStage.bulkCreate(req);
    const [dataValues] = stagingData;
    return { dataValues };
  } catch (error) {
    logger.error(error);
  }
};

export const contentStageMetaData = async (req: any): Promise<any> => {
  try {
    const Contents = await ContentStage.findAll({ where: req });
    const contents = Contents.map((c) => c.dataValues);
    return contents;
  } catch (error) {
    const err = error instanceof Error;
    const errorMsg = err ? error.message || 'failed to get a record' : '';
    return { error: true, message: errorMsg };
  }
};

export const updateContentStage = async (whereClause: any, req: any): Promise<any> => {
  try {
    const updateContent = await ContentStage.update(req, { where: whereClause });
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
