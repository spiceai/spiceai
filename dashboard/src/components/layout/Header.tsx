import React from 'react';
import { Link } from 'react-router-dom';
import { ReactComponent as Logo } from '../../assets/svg/spice-ai-logo.svg';

const Header: React.FunctionComponent = () => {
  return (
    <div className="flex-grow grid grid-cols-3 h-10 items-center bg-primary-dark">
      <div className="justify-self-left">
        <Link to={'/'}>
          <Logo className="h-2.5 w-32 ml-3" />
        </Link>
      </div>
      <div className="justify-self-center"></div>
      <div className=" mr-4 justify-self-end">
        <span className="uppercase font-bold space-x-2 text-xs text-white">
          <a href="https://docs.spiceai.org">Docs</a>
        </span>
      </div>
    </div>
  );
};

export default Header;
