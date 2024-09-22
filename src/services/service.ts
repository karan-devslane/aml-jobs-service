import { boardMaster } from '../models/boardMaster';
import { classMaster } from '../models/classMaster';
import { Repository } from '../models/repository';
import { SkillMaster } from '../models/skill';
import { SubSkillMaster } from '../models/subSkillMaster';
import { Tenant } from '../models/tenant';

export const getTenants = async (): Promise<any> => {
  const Tenants = await Tenant.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  const tenants = Tenants.map((tenant) => ({
    id: tenant.dataValues.id,
    name: JSON.parse(tenant.dataValues.name),
  }));

  return tenants;
};

export const getBoards = async (): Promise<any> => {
  const Boards = await boardMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  const boards = Boards.map((board) => ({
    id: board.dataValues.id,
    name: board.dataValues.name,
  }));

  return boards;
};

export const getClasses = async (): Promise<any> => {
  const Classes = await classMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  const classes = Classes.map((c) => ({
    id: c.dataValues.id,
    name: c.dataValues.name,
  }));

  return classes;
};

export const getSkills = async (): Promise<any> => {
  const Skills = await SkillMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name', 'type'] },
  });

  const skills = Skills.map((skill) => ({
    id: skill.dataValues.id,
    name: skill.dataValues.name,
    type: skill.dataValues.type,
  }));

  return skills;
};

export const getSubSkills = async (): Promise<any> => {
  const SubSkills = await SubSkillMaster.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  const subSkills = SubSkills.map((subSkill) => ({
    id: subSkill.dataValues.id,
    name: subSkill.dataValues.name,
  }));

  return subSkills;
};

export const getRepository = async (): Promise<any> => {
  const Repositories = await Repository.findAll({
    where: { is_active: true },
    attributes: { include: ['id', 'name'] },
  });

  const repositories = Repositories.map((repo) => ({
    id: repo.dataValues.id,
    name: repo.dataValues.name,
  }));

  return repositories;
};
