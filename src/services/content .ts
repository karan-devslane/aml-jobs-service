import { Content } from '../models/content'; // Import Content model

// Create a new content
export const createContent = async (insertData: Record<string, unknown>[]): Promise<any> => {
  const insertContent = await Content.bulkCreate(insertData);
  return { insertContent };
};
