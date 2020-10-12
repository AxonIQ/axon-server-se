import React from 'react';
import { Typography } from './Typography';

export default {
    title: 'Components/Typography',
    component: Typography,
};

export const Primary = () => (
    <div>
        <div>
            <Typography addMargin size="xxxl" tag="h1">
                h1. xxxl. Heading
            </Typography>
        </div>
        <div>
            <Typography addMargin size="xxl" tag="h2">
                h2. xxl. Heading
            </Typography>
        </div>
        <div>
            <Typography addMargin size="xl" tag="h3">
                h3. xl. Heading
            </Typography>
        </div>
        <div>
            <Typography addMargin size="l" tag="h4">
                h4. l. Heading
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" tag="h5">
                h5. m. Heading
            </Typography>
        </div>
        <div>
            <Typography addMargin size="s" tag="h6">
                h6. s. Heading
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" tag="div">
                div. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" weight="bold" tag="div">
                div. bold. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" weight="light" color="light" tag="div">
                div. light. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" weight="lighter" color="lighter" tag="div">
                div. lighter. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" color="primary" tag="div">
                div. lighter. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" color="secondary" tag="div">
                div. lighter. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" italic tag="div">
                div. italic. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" tag="span">
                span. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
            </Typography>
        </div>
        <div>
            <Typography addMargin size="m" tag="p">
                p. Lorem ipsum dolor sit amet, consectetur adipisicing elit. Quos blanditiis tenetur
                unde suscipit, quam beatae rerum inventore consectetur, neque doloribus, cupiditate numquam
                dignissimos laborum fugiat deleniti? Eum quasi quidem quibusdam.
            </Typography>
        </div>
    </div>
  );