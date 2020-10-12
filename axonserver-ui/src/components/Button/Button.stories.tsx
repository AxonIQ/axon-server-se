import React from 'react';
import { Button } from './Button';

export default {
    title: 'Components/Button',
    component: Button,
}

export const Primary = () => <Button type="primary">Button</Button>;

export const Secondary = () => <Button type="secondary">Button</Button>;
