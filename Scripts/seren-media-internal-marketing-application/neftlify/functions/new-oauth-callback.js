const axios = require('axios');

exports.handler = async function(event, context) {
  const clientId = 'sq0idp-XJCGaN6csDILfjspDU5ZHg';  // Your Square Application ID
  const clientSecret = 'sq0csp-XCoa-b0eTwQINTL6FNo1JJzRbGEepfMG7OJ7591Puzs';  // Your Square Application Secret
  const redirectUri = 'https://66e74d0594e4b1549b1c406b--seren-media-internal-marketing-app.netlify.app/.netlify/functions/oauth-callback';  // The same URL you are using as your redirect

  // Get the authorization code from the query parameters
  const authorizationCode = event.queryStringParameters.code;

  if (!authorizationCode) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: 'Authorization code is missing' })
    };
  }

  try {
    // Exchange the authorization code for access and refresh tokens
    const response = await axios.post('https://connect.squareup.com/oauth2/token', {
      client_id: clientId,
      client_secret: clientSecret,
      code: authorizationCode,
      grant_type: 'authorization_code',
      redirect_uri: redirectUri
    });

    const { access_token, refresh_token } = response.data;

    // Return a success message with the tokens
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'OAuth process complete. Tokens received.',
        access_token: access_token,
        refresh_token: refresh_token
      })
    };

  } catch (error) {
    console.error('Error exchanging authorization code:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: 'Error processing the OAuth callback',
        error: error.message
      })
    };
  }
};