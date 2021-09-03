import React from 'react';
import { Link } from 'react-router-dom'

const appVersion = process.env.REACT_APP_SPICE_VERSION

const Footer: React.FunctionComponent = () => {
  return (
    <footer className="flex flex-row gap-2 text-sm text-footer m-2 bg-primary-dark text-right text-xs">
      <div>v{appVersion}</div>
      <div className="flex-grow"></div>
      <div>
        <Link to="/acknowledgements">Acknowledgements</Link>
      </div>
      <div>|</div>
      <div>
        <a href="https://spiceai.org" className="text-primary">spiceai.org</a>
      </div>
    </footer>
  );
};

export default Footer;
