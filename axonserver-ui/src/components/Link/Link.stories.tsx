import React from 'react';
import { Link } from './Link';
import { Typography } from '../Typography/Typography';

export default {
    title: 'Components/Link',
    component: Link,
};

export const Primary = () => <Typography size="m"><Link to="https://duckduckgo.com/" type="primary">Click me</Link></Typography>
export const PrimaryActive = () => <Typography size="m"><Link to="https://duckduckgo.com/" active type="primary">Click me</Link></Typography>
export const Secondary = () => <Typography size="m"><Link to="https://duckduckgo.com/" type="secondary">Click me</Link></Typography>
export const SecondaryActive = () => <Typography size="m"><Link to="https://duckduckgo.com/" active type="secondary">Click me</Link></Typography>
export const Underlined = () => <Typography size="m"><Link to="https://duckduckgo.com/" type="primary" underline>Click me</Link></Typography>
export const Button = () => <Typography size="m"><Link to="https://duckduckgo.com/" type="button">Click me</Link></Typography>
export const OpenInNewPage = () => <Typography size="m"><Link to="https://duckduckgo.com/" target="_blank" type="primary">Open me in a new page</Link></Typography>