import React from 'react';
import { SPICE_VERSION } from '../../constants'

const Footer: React.FunctionComponent = () => {
  return (
    <footer className="flex flex-row gap-2 text-sm text-footer m-2 bg-primary-dark text-right text-xs">
      <div>v{SPICE_VERSION}</div>
      <div className="flex-grow"></div>
      <div>
        <a href="/api/v0.1/acknowledgements">Acknowledgements</a>
      </div>
      <div>|</div>
      <div>
        <a href="https://spiceai.org" className="text-primary">spiceai.org</a>
      </div>
    </footer>
  );
};

export default Footer;
