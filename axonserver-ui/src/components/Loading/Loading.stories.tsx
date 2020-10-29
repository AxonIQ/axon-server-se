import React from 'react';
import { Loading } from './Loading';

export default {
  title: 'Components/Loading',
  component: Loading,
};

export const Primary = () => <Loading />;
export const Secondary = () => <Loading color="secondary" />;
