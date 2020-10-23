import React, { useState } from 'react';
import { Checkbox } from './Checkbox';

export default {
  title: 'Components/Checkbox',
  component: Checkbox,
};

export const Primary = () => <Checkbox label="Primary" color="primary" />;
export const Secondary = () => <Checkbox label="Secondary" color="secondary" />;
export const Default = () => <Checkbox label="Default" color="default" />;
export const WithEffect = () => {
  const [value, setValue] = useState(false);
  return (
    <Checkbox
      label="Check Me"
      checked={value}
      onChange={(_, checked) => {
        setValue(checked);
        if (checked) {
          return alert('You checked the checkbox! Hooray! 🎉');
        }
        alert('You unchecked the checkbox 😥');
      }}
    />
  );
};
