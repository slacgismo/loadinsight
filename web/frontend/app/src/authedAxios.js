import axios from 'axios';

let authedAxios = {};

function _authed_config(config) {
  const token = localStorage.getItem('auth_token');
  if (config === null || config === undefined) {
    return {headers: {'Authorization': `Token ${token}`}};
  } else {
    config.headers['Authorization'] = `Token ${token}`;
    return config;
  }
}

authedAxios.get = (url, config) => {
  return axios.get(url, _authed_config(config));
}

authedAxios.post = (url, data, config) => {
  return axios.post(url, data, _authed_config(config));
}

export default authedAxios;