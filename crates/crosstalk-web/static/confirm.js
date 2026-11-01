// Confirmation dialogs for the forms that carry a data-confirm attribute.
//
// A delegated submit listener, so a strict Content-Security-Policy (script-src 'self',
// no 'unsafe-inline') still allows the orange and red buttons to ask first. The template
// renders the message into data-confirm; this reads it back and cancels the submit when
// the operator declines. Server-side confirmation (confirm=true) is still required: this
// dialog is convenience, not protection.
document.addEventListener("submit", function (event) {
  var form = event.target;
  if (!(form instanceof HTMLFormElement)) {
    return;
  }
  var message = form.getAttribute("data-confirm");
  if (message && !window.confirm(message)) {
    event.preventDefault();
  }
});
