import React from 'react';
import { Link } from 'react-router-dom'
import { SPICE_VERSION } from '../../constants'

const Footer: React.FunctionComponent = () => {
  return (
    <div className="flex flex-grow flex-row gap-2 text-sm text-footer m-2 bg-primary-dark text-right text-xs">
      <div>{SPICE_VERSION}</div>
      <div className="flex-grow"></div>
      <div>
        <Link to="/acknowledgements">Acknowledgements</Link>
      </div>
      <div>|</div>
      <div>
        <a href="https://spiceai.org" className="text-primary">spiceai.org</a>
      </div>
    </div>
  );
};

export default Footer;
