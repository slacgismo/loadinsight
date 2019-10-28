import React from 'react';
import Avatar from '@material-ui/core/Avatar';
import CssBaseline from '@material-ui/core/CssBaseline';
import Grid from '@material-ui/core/Grid';
import Box from '@material-ui/core/Box';
import EmailIcon from '@material-ui/icons/Email';
import Typography from '@material-ui/core/Typography';
import { makeStyles } from '@material-ui/core/styles';
import Container from '@material-ui/core/Container';
import {Link as RouterLink} from "react-router-dom";
import { useState, useEffect } from 'react';
import axios from 'axios';

function Copyright() {
  return (
    <Typography variant="body2" color="textSecondary" align="center">
      {'Copyright © '}
      LoadInsight {' '}
      {new Date().getFullYear()}
      {'.'}
    </Typography>
  );
}

const useStyles = makeStyles(theme => ({
  '@global': {
    body: {
      backgroundColor: theme.palette.common.white,
    },
  },
  paper: {
    marginTop: theme.spacing(8),
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
  },
  avatar: {
    margin: theme.spacing(1),
    backgroundColor: theme.palette.secondary.main,
  },
  form: {
    width: '100%', // Fix IE 11 issue.
    marginTop: theme.spacing(1),
  },
  submit: {
    margin: theme.spacing(3, 0, 2),
  },
}));

export default function ActivateEmail(props) {
  const [emailActivated, setEmailActivated] = useState(null);

  useEffect(() => {
    axios.post('/auth/users/activation/', {
      uid: props.match.params.uid,
      token: props.match.params.token
    }).then(response => {
      setEmailActivated(true);
    }).catch(error => {
      setEmailActivated(false);
    });
  }, []);

  let content = ``;
  let link = '';
  let linkText = ``;

  if (emailActivated !== null) {
    if (emailActivated === true) {
      content = `Your account has been activated!`;
      link = '/signin';
      linkText = 'Sign In';
    } else {
      content = `The activation link is either broken or expired, please try wtih a valid one.`;
      link = '/signup';
      linkText = 'Sign Up';
    }
  }


  const classes = useStyles();
  return (
    <Container component="main" maxWidth="xs">
      <CssBaseline />
      <div className={classes.paper}>
        <Avatar className={classes.avatar}>
          <EmailIcon />
        </Avatar>
        <Typography variant="body1">
          { content }
        </Typography>
        <Grid container justify="center">
          <Grid item>
            <RouterLink to={link} variant="body1">
              {linkText}
            </RouterLink>
          </Grid>
        </Grid>
      </div>
      <Box mt={8}>
        <Copyright />
      </Box>
    </Container>
  );
}