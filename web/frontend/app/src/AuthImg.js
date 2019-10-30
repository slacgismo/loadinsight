import React from 'react';
import useSWR from "@zeit/swr";
import ClipLoader from 'react-spinners/ClipLoader';

export default ({src, ...other}) => {
  const token = localStorage.getItem('auth_token');
  const { data: dataURL, error } = useSWR(
    src,
     async (key) => {
      const image = await fetch(key, {
        headers: new Headers([['Authorization', `Token ${token}`], ['responseType', 'blob']])});
      const imageBlob = await image.blob();
      const reader = new FileReader();
      return new Promise((resolve, reject) => {
        reader.onloadend = () => {
          resolve(reader.result)};
          reader.readAsDataURL(imageBlob);
      });
    },
    {
      revalidateOnFocus: false,
      refreshWhenHidden: false,
      shouldRetryOnError: false
    }
  );
  if (error) return <div>{error}</div>;
  if (!dataURL) return <ClipLoader/>;

  return <img src={dataURL} {...other} alt=''/>;
  }
