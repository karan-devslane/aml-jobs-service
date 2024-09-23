import { Content } from '../models/content';

export const createContent = async (insertData: Record<string, unknown>[]): Promise<any> => {
  const insertContent = await Content.bulkCreate(insertData);
  return { insertContent };
};
