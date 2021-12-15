export interface IErrorAlertProps {
  title?: string
  message: string
  onClose?: () => void
}

const ErrorAlert: React.FunctionComponent<IErrorAlertProps> = (props) => {
  const { title, message, onClose } = props

  return (
    <div
      className="bg-red-100 border border-red-400 text-red-700 px-3 py-2 rounded relative text-sm"
      role="alert"
    >
      <strong className="font-bold">{title || 'Error'}:</strong>
      <span className="ml-2 block sm:inline">{message}</span>
      <span className="absolute top-0 bottom-0 right-0 px-3 py-2" onClick={onClose}>
        <svg
          className="fill-current h-5 w-5 text-red-500"
          role="button"
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 20 20"
        >
          <title>Close</title>
          <path d="M14.348 14.849a1.2 1.2 0 0 1-1.697 0L10 11.819l-2.651 3.029a1.2 1.2 0 1 1-1.697-1.697l2.758-3.15-2.759-3.152a1.2 1.2 0 1 1 1.697-1.697L10 8.183l2.651-3.031a1.2 1.2 0 1 1 1.697 1.697l-2.758 3.152 2.758 3.15a1.2 1.2 0 0 1 0 1.698z" />
        </svg>
      </span>
    </div>
  )
}

export default ErrorAlert
