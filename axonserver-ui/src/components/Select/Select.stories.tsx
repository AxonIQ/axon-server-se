import React, { useState } from 'react';
import { FormControl } from '../../components/FormControl/FormControl';
import { InputLabel } from '../../components/InputLabel/InputLabel';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { Select } from '../../components/Select/Select';

export default {
  title: 'Components/Select',
  component: Select,
};

export const Default = () => {
  const dropdownValues = [10, 20, 30];
  const [value, setValue] = useState('');

  return (
    <FormControl fullWidth>
      <InputLabel id="demo-simple-select-label">
        Please select a value form the dropdown
      </InputLabel>
      <Select
        labelId="demo-simple-select-label"
        id="demo-simple-select"
        value={value}
        onChange={(event) => {
          setValue(event.target.value as string);
        }}
      >
        {dropdownValues.map((value, index) => (
          <MenuItem key={`value${index}`} value={value}>
            {value}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};
