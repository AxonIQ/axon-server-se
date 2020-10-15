import React from 'react';
import { default as MUiCard } from '@material-ui/core/Card';
import { default as MUiCardContent } from '@material-ui/core/CardContent';

type CardProps = {
  children: React.ReactNode;
};
export const Card = (prop: CardProps) => (
  <MUiCard elevation={0}>
    <MUiCardContent>{prop.children}</MUiCardContent>
  </MUiCard>
);
