import { DataTypes } from 'sequelize';
import { AppDataSource } from '../config';

export const Repository = AppDataSource.define(
  'repository',
  {
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true,
    },
    identifier: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    name: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    tenant_id: {
      type: DataTypes.ARRAY(DataTypes.INTEGER),
      allowNull: true,
    },
    is_active: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM('draft', 'live'),
      allowNull: false,
    },
    created_by: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    updated_by: {
      type: DataTypes.STRING,
      allowNull: true,
    },
  },
  {
    tableName: 'repository',
    timestamps: true,
    createdAt: 'created_at',
    updatedAt: 'updated_at',
  },
);
