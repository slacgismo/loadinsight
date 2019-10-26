import React from 'react';
import Avatar from '@material-ui/core/Avatar';
import Button from '@material-ui/core/Button';
import CssBaseline from '@material-ui/core/CssBaseline';
import Grid from '@material-ui/core/Grid';
import Box from '@material-ui/core/Box';
import LockOutlinedIcon from '@material-ui/icons/LockOutlined';
import Typography from '@material-ui/core/Typography';
import { makeStyles } from '@material-ui/core/styles';
import Container from '@material-ui/core/Container';
import {Link as RouterLink} from "react-router-dom";
import { Formik, Field, Form } from 'formik';
import { TextField } from 'formik-material-ui';
import * as Yup from 'yup';

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
    marginTop: theme.spacing(3),
  },
  submit: {
    margin: theme.spacing(3, 0, 2),
  },
}));

const SignupSchema = Yup.object().shape({
  username: Yup.string().required('Required'),
  email: Yup.string().email('Invalid email').required('Required'),
  password: Yup.string().required('Required'),
  repeatPassword: Yup.string().required('Required')
    .test('same-passwords', 'Passwords should be the same', function (val) {
    return this.parent.password === val;
  })
})

export default function SignUp() {
  const classes = useStyles();

  return (
    <Container component="main" maxWidth="xs">
      <CssBaseline />
      <div className={classes.paper}>
        <Avatar className={classes.avatar}>
          <LockOutlinedIcon />
        </Avatar>
        <Typography component="h1" variant="h5">
          Sign up
        </Typography>

        <Formik
          initialValues={{
            username: '',
            email: '',
            password: '',
            repeatPassword: ''
        }}
          validationSchema={SignupSchema}
          onSubmit={({username, email, password}, actions) => {
            fetch('/api/signup/', {
              method: 'post',
              headers: {
                "Content-Type": "application/json"
              },
              body: JSON.stringify({username, email, password})
            }).then(response => {
              console.log(response);
            })
          }}
        >
          {({errors, status, touched, isSubmitting}) => (

        <Form className={classes.form}>
          <Grid container spacing={2}>
            <Grid item xs={12}>
              <Field
                type="text"
                name="username"
                label="Username"
                component={TextField}
                autoComplete="username"
                variant="outlined"
                required
                fullWidth
                id="username"
                autoFocus
              />
            </Grid>
            <Grid item xs={12}>
              <Field
                type="email"
                label="Email Address"
                name="email"
                component={TextField}
                variant="outlined"
                required
                fullWidth
                id="email"
                autoComplete="email"
              />
            </Grid>
            <Grid item xs={12}>
              <Field
                name="password"
                label="Password"
                type="password"
                component={TextField}
                variant="outlined"
                required
                fullWidth
                id="password"
                autoComplete="current-password"
              />
            </Grid>
            <Grid item xs={12}>
              <Field
                name="repeatPassword"
                label="Repeat Password"
                type="password"
                component={TextField}
                variant="outlined"
                required
                fullWidth
                id="repeatPassword"
              />
            </Grid>
          </Grid>
          <Button
            type="submit"
            fullWidth
            variant="contained"
            color="primary"
            className={classes.submit}
          >
            Sign Up
          </Button>
          <Grid container justify="flex-end">
            <Grid item>
              <RouterLink to="/signin" variant="body2">
                  Already have an account? Sign in
              </RouterLink>
            </Grid>
          </Grid>
        </Form>
          )}
        </Formik>

      </div>
      <Box mt={5}>
        <Copyright />
      </Box>
    </Container>
  );
}