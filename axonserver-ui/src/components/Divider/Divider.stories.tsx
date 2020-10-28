import React from 'react';
import { Typography } from '../Typography/Typography';
import { Divider } from './Divider';

export default {
  title: 'Components/Divider',
  component: Divider,
};

export const Default = () => (
  <div>
    <Typography size="m">I am above</Typography>
    <Divider />
    <Typography size="m">I am below</Typography>
  </div>
);
