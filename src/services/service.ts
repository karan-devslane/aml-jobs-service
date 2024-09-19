import { boardMaster } from '../models/boardMaster';
import { classMaster } from '../models/classMaster';
import { Repository } from '../models/repository';
import { SkillMaster } from '../models/skill';
import { SubSkillMaster } from '../models/subSkillMaster';
import { Tenant } from '../models/tenant';

export const getTenants = async (): Promise<any> => {
  const tenants = await Tenant.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  return { tenants };
};

export const getBoards = async (): Promise<any> => {
  const boards = await boardMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  return { boards };
};

export const getClasses = async (): Promise<any> => {
  const classes = await classMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  return { classes };
};

export const getSkills = async (): Promise<any> => {
  const skills = await SkillMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  return { skills };
};

export const getSubSkills = async (): Promise<any> => {
  const subSkills = await SubSkillMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  return { subSkills };
};

export const getRepository = async (): Promise<any> => {
  const repositories = await Repository.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  return { repositories };
};
