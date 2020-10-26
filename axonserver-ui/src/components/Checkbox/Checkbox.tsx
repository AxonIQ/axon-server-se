import { FormControlLabel } from '@material-ui/core';
import { default as MUiCheckbox } from '@material-ui/core/Checkbox';
import React from 'react';
import { Typography } from '../Typography/Typography';

type CheckboxProps = {
  label?: string;
  checked?: boolean;
  onChange?: (
    event: React.ChangeEvent<HTMLInputElement>,
    checked: boolean,
  ) => void;
  color?: 'primary' | 'secondary' | 'default';
  name?: string;
};
export const Checkbox = (props: CheckboxProps) => {
  return (
    <FormControlLabel
      control={
        <MUiCheckbox
          name={props.name}
          color={props.color}
          checked={props.checked}
          onChange={props.onChange}
        />
      }
      label={
        <Typography size="m" color="light">
          {props.label}
        </Typography>
      }
    />
  );
};
